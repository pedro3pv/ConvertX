import * as fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { stringify } from "csv-stringify";
import { parquetMetadataAsync, parquetRead } from "hyparquet";

export const properties = {
  from: {
    data: ["parquet"],
  },
  to: {
    data: ["csv"],
  },
};

export async function convert(
  filePath: string,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  fileType: string,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  convertTo: string,
  targetPath: string,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  options?: unknown,
): Promise<string> {
  const fileHandle = await fs.open(filePath, "r");
  try {
    const stat = await fileHandle.stat();
    const file = {
      byteLength: stat.size,
      async slice(start: number, end?: number): Promise<ArrayBuffer> {
        const length = (end ?? stat.size) - start;
        const buf = Buffer.allocUnsafe(length);
        const { bytesRead } = await fileHandle.read(buf, 0, length, start);
        return buf.buffer.slice(buf.byteOffset, buf.byteOffset + bytesRead);
      },
    };

    // Use the async version of metadata reader
    const metadata = await parquetMetadataAsync(file as any);
    const stringifier = stringify({ header: true });
    const writeStream = createWriteStream(targetPath);

    return new Promise((resolve, reject) => {
      stringifier.pipe(writeStream);

      writeStream.on("finish", async () => {
        await fileHandle.close();
        resolve("Done");
      });
      writeStream.on("error", async (err) => {
        await fileHandle.close();
        reject(err);
      });
      stringifier.on("error", async (err) => {
        await fileHandle.close();
        reject(err);
      });

      (async () => {
        try {
          let rowStart = 0;
          for (const rowGroup of metadata.row_groups) {
            const numRows = Number(rowGroup.num_rows);
            const rowEnd = rowStart + numRows;
            
            await parquetRead({
              file: file as any,
              rowStart,
              rowEnd,
              rowFormat: "object",
              onComplete: (rows) => {
                if (Array.isArray(rows)) {
                  for (const row of rows) {
                    stringifier.write(row);
                  }
                }
              },
            });
            rowStart = rowEnd;
          }
          stringifier.end();
        } catch (err) {
          stringifier.destroy(err as Error);
        }
      })();
    });
  } catch (err) {
    await fileHandle.close();
    throw err;
  }
}


