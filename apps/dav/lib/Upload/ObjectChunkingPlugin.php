<?php
/**
 * @copyright Copyright (c) 2017, ownCloud GmbH
 *
 * @author Christoph Wurst <christoph@winzerhof-wurst.at>
 * @author Daniel Kesselberg <mail@danielkesselberg.de>
 * @author Julius Härtl <jus@bitgrid.net>
 * @author Morris Jobke <hey@morrisjobke.de>
 * @author Thomas Müller <thomas.mueller@tmit.eu>
 *
 * @license AGPL-3.0
 *
 * This code is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License, version 3,
 * along with this program. If not, see <http://www.gnu.org/licenses/>
 *
 */

namespace OCA\DAV\Upload;

use OC\Files\Filesystem;
use OC\Files\ObjectStore\ObjectStoreStorage;
use OC\Files\View;
use OCA\DAV\Connector\Sabre\Directory;
use OCA\DAV\Connector\Sabre\Exception\Forbidden;
use OCA\DAV\Connector\Sabre\File;
use OCP\Files\ObjectStore\IObjectStoreMultiPartUpload;
use Sabre\DAV\Exception\BadRequest;
use Sabre\DAV\Exception\NotFound;
use Sabre\DAV\INode;
use Sabre\DAV\Server;
use Sabre\DAV\ServerPlugin;
use Sabre\HTTP\RequestInterface;
use Sabre\HTTP\ResponseInterface;

class ObjectChunkingPlugin extends ServerPlugin {

	/** @var Server */
	private $server;
	/** @var UploadFolder */
	private $uploadFolder;

	const OBJECT_UPLOAD_URN = '{http://nextcloud.org/ns}object-upload-urn';
	const OBJECT_UPLOAD_UPLOADID = '{http://nextcloud.org/ns}object-upload-uploadid';
	const OBJECT_UPLOAD_PARTID = '{http://nextcloud.org/ns}object-upload-partid';
	const OBJECT_UPLOAD_ETAG = '{http://nextcloud.org/ns}object-upload-etag';

	const OBJECT_UPLOAD_HEADER = 'X-Nextcloud-Object-Chunking';
	const OBJECT_UPLOAD_DESTINATION_HEADER = 'X-Nextcloud-Object-Destination';

	/**
	 * @inheritdoc
	 */
	public function initialize(Server $server) {
		$server->on('afterMethod:MKCOL', [$this, 'beforeMkcol']);
		$server->on('beforeMethod:PUT', [$this, 'beforePut'], 200);	// Different priority to call after the custom properties backend is registered
		$server->on('beforeMove', [$this, 'beforeMove'], 90);

		$this->server = $server;
	}

	private function checkPrerequisites() {
		if (!$this->uploadFolder instanceof UploadFolder || !$this->server->httpRequest->getHeader(self::OBJECT_UPLOAD_HEADER)) {
			return false;
		}
		$multipartUploader = $this->getMultipartStorage();
		$storage = $this->getStorage();
		return $multipartUploader !== null && $storage !== null;
	}

	public function beforeMkcol(RequestInterface $request, ResponseInterface $response) {
		$this->uploadFolder = $this->server->tree->getNodeForPath($request->getPath());
		if (!$this->checkPrerequisites() || !$this->server->httpRequest->getHeader(self::OBJECT_UPLOAD_DESTINATION_HEADER)) {
			return true;
		}

		$this->uploadFolder->createFile('.multipart');
		try {
			$targetFile = $this->server->tree->getNodeForPath($request->getHeader(self::OBJECT_UPLOAD_DESTINATION_HEADER));
		} catch (NotFound $e) {
			$targetFile = $this->uploadFolder->getChild('.multipart');
		}

		$multipartUploader = $this->getMultipartStorage();
		$storage = $this->getStorage();
		$uploadId = $multipartUploader->initiateMultipartUpload($storage->getURN($targetFile->getInternalFileId()));
		$this->server->updateProperties($request->getPath(), [
			self::OBJECT_UPLOAD_UPLOADID => $uploadId,
			self::OBJECT_UPLOAD_URN => $storage->getURN((int)$targetFile->getInternalFileId())
		]);

		return true;
	}

	public function beforePut(RequestInterface $request, ResponseInterface $response) {
		$this->uploadFolder = $this->server->tree->getNodeForPath(dirname($request->getPath()));
		if (!$this->checkPrerequisites()) {
			return true;
		}

		$multipartUploader = $this->getMultipartStorage();
		$properties = $this->server->getProperties(dirname($request->getPath()) . '/', [ self::OBJECT_UPLOAD_UPLOADID, self::OBJECT_UPLOAD_URN ]);
		$urn = $properties[self::OBJECT_UPLOAD_URN];
		$uploadId = $properties[self::OBJECT_UPLOAD_UPLOADID];
		$partId = (int)basename($request->getPath());
		if (!($partId >= 1 && $partId <= 10000)) {
			throw new BadRequest('Invalid chunk id');
		}

		$stream = $request->getBodyAsStream();
		$result = $multipartUploader->uploadMultipartPart($urn, $uploadId, basename($request->getPath()), $stream, $request->getHeader('Content-Length'));

		// Create a fake chunk file so we can store the metadata
		$_SERVER['CONTENT_LENGTH'] = 0;
		$this->uploadFolder->createFile(basename($request->getPath()));

		$this->server->updateProperties($request->getPath(), [
			self::OBJECT_UPLOAD_PARTID => $partId,
			self::OBJECT_UPLOAD_ETAG => trim($result->get('ETag'), '"')
		]);
		$this->server->httpResponse->setStatus(201);
		return false;
	}

	public function beforeMove($sourcePath, $destination) {
		$this->uploadFolder = $this->server->tree->getNodeForPath(dirname($sourcePath));
		if (!$this->checkPrerequisites()) {
			return true;
		}

		$request = $this->server->httpRequest;
		$multipartUploader = $this->getMultipartStorage();
		$storage = $this->getStorage();
		$properties = $this->server->getProperties(dirname($request->getPath()) . '/', [self::OBJECT_UPLOAD_UPLOADID, self::OBJECT_UPLOAD_URN]);
		$urn = $properties[self::OBJECT_UPLOAD_URN];
		$uploadId = $properties[self::OBJECT_UPLOAD_UPLOADID];

		try {
			$props = $this->server->getPropertiesForChildren(dirname($request->getPath()), [
				self::OBJECT_UPLOAD_PARTID,
				self::OBJECT_UPLOAD_ETAG
			]);
			$parts = array_filter($props, function ($value, $key) {
				return substr_compare($key, '/.file', -\strlen('/.file')) !== 0 &&
					substr_compare($key, '/.multipart', -\strlen('/.multipart')) !== 0;
			}, ARRAY_FILTER_USE_BOTH);
			ksort($parts);
			$partData = array_map(function ($props) {
				return [
					'ETag' => $props[self::OBJECT_UPLOAD_ETAG],
					'PartNumber' => $props[self::OBJECT_UPLOAD_PARTID]
				];
			}, $parts);

			$rootView = new View();
			$sourceInView = $this->server->tree->getNodeForPath(dirname($sourcePath))->getChild('.multipart')->getPath();

			list($destinationDir, $destinationName) = \Sabre\Uri\split($destination);
			/** @var Directory $destinationParent */
			$destinationParent = $this->server->tree->getNodeForPath($destinationDir);
			$destinationExists = $destinationParent->childExists($destinationName);
			$destinationInView = $destinationParent->getFileInfo()->getPath() . '/' . $destinationName;

			if ($destinationExists) {
				$rootView->file_put_contents($destinationInView, ''); // FIXME: Workaround to trigger version creation through pre hooks
			}
			$multipartUploadSize = $multipartUploader->completeMultipartUpload($urn, $uploadId, array_values($partData));
			if (!$destinationExists) {
				$rootView->rename(dirname($sourceInView) . '/.multipart', $destinationInView);
			}

			$destinationInView = $this->server->tree->getNodeForPath($destination)->getFileInfo()->getPath();

			$mimetypeDetector = \OC::$server->getMimeTypeDetector();
			$mimetype = $mimetypeDetector->detectPath($destinationInView);
			$destinationFileInfo = $rootView->getFileInfo($destinationInView);
			$storage->getCache()->update($destinationFileInfo->getId(), [
				'size' => $multipartUploadSize,
				'mimetype' => $mimetype,
				'etag' => $storage->getETag($destinationFileInfo->getInternalPath())
			]);
		} catch (\Exception $e) {
			$multipartUploader->abortMultipartUpload($urn, $uploadId);
			throw $e;
		} finally {
			$sourceNode = $this->server->tree->getNodeForPath($sourcePath);
			if ($sourceNode instanceof FutureFile) {
				$sourceNode->delete();
			}

			$this->server->emit('afterMove', [$sourcePath, $destination]);
			$this->server->emit('afterUnbind', [$sourcePath]);
			$this->server->emit('afterBind', [$destination]);

			$response = $this->server->httpResponse;
			$response->setHeader('Content-Length', '0');
			$response->setStatus($destinationExists ? 204 : 201);
		}
		return false;
	}

	private function getMultipartStorage() {
		$storage = $this->getStorage();
		if (!$storage) {
			return null;
		}
		$objectStore = $storage->getObjectStore();
		return $objectStore instanceof IObjectStoreMultiPartUpload ? $objectStore : null;
	}

	private function getStorage() {
		$storage = $this->uploadFolder->getStorage();
		return $storage->instanceOfStorage(ObjectStoreStorage::class) ? $storage : null;
	}
}
