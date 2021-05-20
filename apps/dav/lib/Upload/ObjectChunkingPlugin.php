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
use OCP\Files\Storage\IChunkedFileWrite;
use OCP\Files\Storage\IStorage;
use OCP\Files\StorageInvalidException;
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

	private const TEMP_TARGET = '.target';

	const OBJECT_UPLOAD_TARGET = '{http://nextcloud.org/ns}upload-target';
	const OBJECT_UPLOAD_UPLOADID = '{http://nextcloud.org/ns}upload-uploadid';

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

	public function beforeMkcol(RequestInterface $request, ResponseInterface $response) {
		$this->uploadFolder = $this->server->tree->getNodeForPath($request->getPath());
		try {
			$this->checkPrerequisites();
			$storage = $this->getStorage();
		} catch (StorageInvalidException|BadRequest $e) {
			return true;
		}

		// TODO: If the target is not set we could still copy the file during the move then as a fallback
		if (!$this->server->httpRequest->getHeader(self::OBJECT_UPLOAD_DESTINATION_HEADER)) {
			return true;
		}

		try {
			$targetFile = $this->server->tree->getNodeForPath($request->getHeader(self::OBJECT_UPLOAD_DESTINATION_HEADER));
		} catch (NotFound $e) {
			$this->uploadFolder->createFile(self::TEMP_TARGET);
			$targetFile = $this->uploadFolder->getChild(self::TEMP_TARGET);
		}

		$targetPath = $targetFile->getInternalPath();
		$uploadId = $storage->beginChunkedFile($targetPath);

		// DAV properties on the UploadFolder are used in order to properly cleanup stale chunked file writes and to persist the target path
		$this->server->updateProperties($request->getPath(), [
			self::OBJECT_UPLOAD_UPLOADID => $uploadId,
			self::OBJECT_UPLOAD_TARGET => $targetPath,
		]);

		$response->setStatus(201);
		return true;
	}

	public function beforePut(RequestInterface $request, ResponseInterface $response) {
		$this->uploadFolder = $this->server->tree->getNodeForPath(dirname($request->getPath()));
		try {
			$this->checkPrerequisites();
			$storage = $this->getStorage();
		} catch (StorageInvalidException|BadRequest $e) {
			return true;
		}

		$properties = $this->server->getProperties(dirname($request->getPath()) . '/', [ self::OBJECT_UPLOAD_UPLOADID, self::OBJECT_UPLOAD_TARGET ]);
		$targetPath = $properties[self::OBJECT_UPLOAD_TARGET];
		$uploadId = $properties[self::OBJECT_UPLOAD_UPLOADID];
		$partId = (int)basename($request->getPath());

		if (!($partId >= 1 && $partId <= 10000)) {
			throw new BadRequest('Invalid chunk id');
		}

		$storage->putChunkedFilePart($targetPath, $uploadId, (string)$partId, $request->getBodyAsStream(), $request->getHeader('Content-Length'));
		$response->setStatus(201);
		return false;
	}

	public function beforeMove($sourcePath, $destination) {
		$this->uploadFolder = $this->server->tree->getNodeForPath(dirname($sourcePath));
		try {
			$this->checkPrerequisites();
			$this->getStorage();
		} catch (StorageInvalidException|BadRequest $e) {
			return true;
		}
		$properties = $this->server->getProperties(dirname($sourcePath) . '/', [ self::OBJECT_UPLOAD_UPLOADID, self::OBJECT_UPLOAD_TARGET ]);
		$targetPath = $properties[self::OBJECT_UPLOAD_TARGET];
		$uploadId = $properties[self::OBJECT_UPLOAD_UPLOADID];

		list($destinationDir, $destinationName) = \Sabre\Uri\split($destination);
		/** @var Directory $destinationParent */
		$destinationParent = $this->server->tree->getNodeForPath($destinationDir);
		$destinationExists = $destinationParent->childExists($destinationName);
		$destinationInView = $destinationParent->getFileInfo()->getPath() . '/' . $destinationName;

		$rootView = new View();
		// FIXME find a cleaner way to trigger the locking and hooks
		$rootView->file_put_contents($destinationInView, function ($storage, $pathInStorage) use ($targetPath, $uploadId) {
			try {
				$storage->writeChunkedFile($targetPath, $uploadId);
			} catch (\Exception $e) {
				return false;
			}
			return true;
		});
		if (!$destinationExists) {
			$tempFile = $this->server->tree->getNodeForPath(dirname($sourcePath))->getChild(self::TEMP_TARGET);
			$rootView->rename($tempFile->getFile()->getFileInfo()->getPath(), $destinationInView);
		}

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
		return false;
	}

	private function checkPrerequisites() {
		if (!$this->uploadFolder instanceof UploadFolder || !$this->server->httpRequest->getHeader(self::OBJECT_UPLOAD_HEADER)) {
			throw new BadRequest('Object upload header not set');
		}
	}

	/**
	 * @return IChunkedFileWrite
	 * @throws BadRequest
	 * @throws StorageInvalidException
	 */
	private function getStorage(): IStorage {
		$this->checkPrerequisites();
		$storage = $this->uploadFolder->getStorage();
		if (!$storage->instanceOfStorage(IChunkedFileWrite::class)) {
			throw new StorageInvalidException('Storage does not support chunked file write');
		}
		return $storage;
	}
}
