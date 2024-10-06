import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { createReadStream, readdirSync, statSync, createWriteStream } from "fs";

const path = require("path");
const fetch = require("node-fetch");

const cesiumUrl = "https://api.cesium.com/v1";
const authToken = process.env.CESIUM_AUTH_TOKEN;

interface CesiumAsset {
  id?: number;
  archiveId?: number;
  name?: string;
  description?: string;
  type?: "3DTILES";
  options?: {
    sourceType: "3D_MODEL";
    geometryCompression: "NONE";
  };
}

const createNewAssetHandler = async (event: CesiumAsset) => {
  console.log(authToken);
  console.log(JSON.stringify({
    name: event.name,
    description: event.description,
    type: event.type,
    options: {
      sourceType: event.options.sourceType,
      geometryCompression: event.options.geometryCompression,
    },
  }));
  try {
    // Send the POST request with JSON data
    const response = await fetch(`${cesiumUrl}/assets`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      },
      body: JSON.stringify({
        name: event.name,
        description: event.description,
        type: event.type,
        options: {
          sourceType: event.options.sourceType,
          geometryCompression: event.options.geometryCompression,
        },
      }),
    });

    // Check if the response status is OK (200-299)
    if (!response.ok) {
      console.error(response);
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    // Parse the JSON response
    const responseData = await response.json();
    return responseData;
  } catch (error) {
    console.error("Error posting JSON data:", error);
    throw error;
  }
};

const notifyUploadCompleteHandler = async (event: CesiumAsset) => {
  try {
    // Send the POST request with JSON data
    const response = await fetch(
      `${cesiumUrl}/assets/${event.id}/uploadComplete`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${authToken}`,
        },
      }
    );
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    return response;
  } catch (error) {
    console.error("Error posting JSON data:", error);
    throw error;
  }
};

const getStatusHandler = async (event: CesiumAsset) => {
  try {
    const response = await fetch(`${cesiumUrl}/assets/${event.id}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${authToken}`,
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    // Parse the JSON response
    const responseData = await response.json();
    return responseData;
  } catch (error) {
    console.error("Error posting JSON data:", error);
    throw error;
  }
};

const createArchiveHandler = async (event: CesiumAsset) => {
  try {
    const response = await fetch(`${cesiumUrl}/archives`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      },
      body: JSON.stringify({
        assetIds: [event.id],
        format: "ZIP",
        type: "FULL",
      }),
    });

    // Check if the response status is OK (200-299)
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    // Parse the JSON response
    const responseData = await response.json();
    return responseData;
  } catch (error) {
    console.error("Error posting JSON data:", error);
    throw error;
  }
};

const getArchiveStatusHandler = async (event: CesiumAsset) => {
  try {
    const response = await fetch(`${cesiumUrl}/archives/${event.archiveId}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${authToken}`,
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    // Parse the JSON response
    const responseData = await response.json();
    return responseData;
  } catch (error) {
    console.error("Error posting JSON data:", error);
    throw error;
  }
};

const downloadArchiveHandler = async (
  event: CesiumAsset,
  downloadURL: string
) => {
  try {
    const response = await fetch(
      `${cesiumUrl}/archives/${event.archiveId}/download`,
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${authToken}`,
        },
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    const writer = createWriteStream(downloadURL);
    await response.body.pipe(writer);
  } catch (error) {
    console.error("Error posting JSON data:", error);
    throw error;
  }
};

const create3DTiles = async (
  name: string,
  description: string,
  inputDir: string,
  downloadFile: string
) => {
  const createNewAssetResult = await createNewAssetHandler({
    name: name,
    description: description,
    type: "3DTILES",
    options: { sourceType: "3D_MODEL", geometryCompression: "NONE" },
  });
  console.log(createNewAssetResult);

  const AWS_ACCESS_KEY_ID = createNewAssetResult.uploadLocation.accessKey;
  const AWS_SECRET_ACCESS_KEY =
    createNewAssetResult.uploadLocation.secretAccessKey;
  const SESSION_TOKEN = createNewAssetResult.uploadLocation.sessionToken;
  const AWS_REGION = "us-east-1"; // e.g., "us-east-1"
  const BUCKET_NAME = createNewAssetResult.uploadLocation.bucket;

  // Create an S3 client
  const s3Client = new S3Client({
    region: AWS_REGION,
    credentials: {
      accessKeyId: AWS_ACCESS_KEY_ID,
      secretAccessKey: AWS_SECRET_ACCESS_KEY,
      sessionToken: SESSION_TOKEN,
    },
  });

  //const fileStream = createReadStream("/Users/gbiegel/Downloads/test.zip");
  const dir = inputDir;
  const files = readdirSync(dir);

  for (const file of files) {
    try {
      const filePath = path.join(dir, file);

      // Ensure the path is a file (not a directory)
      if (statSync(filePath).isFile()) {
        const fileStream = createReadStream(filePath);

        // Create the S3 PutObjectCommand
        const uploadParams = {
          Bucket: BUCKET_NAME,
          Key: `sources/${createNewAssetResult.assetMetadata.id}/${file}`, // The name of the file (or path in the bucket)
          Body: fileStream,
        };
        // Send the upload request
        const command = new PutObjectCommand(uploadParams);
        const fileUploadResult = await s3Client.send(command);
      }
    } catch (e) {
      console.error(e);
    }
  }

  const notifyUploadCompleteResult = await notifyUploadCompleteHandler({
    id: createNewAssetResult.assetMetadata.id,
  });
  console.log(notifyUploadCompleteResult);

  let statusResult = await getStatusHandler({
    id: createNewAssetResult.assetMetadata.id,
  });
  console.log(statusResult);
  while (statusResult.status != "COMPLETE") {
    statusResult = await getStatusHandler({
      id: createNewAssetResult.assetMetadata.id,
    });
    console.log(statusResult);
  }

  let createArchiveResult = await createArchiveHandler({
    id: createNewAssetResult.assetMetadata.id,
  });
  console.log(createArchiveResult);
  // while (createArchiveResult.status != "COMPLETE") {
  //   createArchiveResult = await createArchiveHandler({
  //     id: createNewAssetResult.assetMetadata.id,
  //   });
  //   console.log(createArchiveResult);
  // }

  let archiveStatusResult = await getArchiveStatusHandler({
    archiveId: createArchiveResult.id,
  });
  console.log(archiveStatusResult);
  while (archiveStatusResult.status != "COMPLETE") {
    archiveStatusResult = await getArchiveStatusHandler({
      archiveId: createArchiveResult.id,
    });
    console.log(archiveStatusResult);
  }

  const downloadResult = await downloadArchiveHandler(
    { archiveId: createArchiveResult.id },
    downloadFile
  );
  console.log(downloadResult);
};

create3DTiles(
  "Test",
  "Test",
  "/tmp",
  "/tmp/output.zip"
);
