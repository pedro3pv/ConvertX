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
  _fileType: string,
  _convertTo: string,
  targetPath: string,
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
    const metadata = await parquetMetadataAsync(file);
    const stringifier = stringify({ header: true });
    const writeStream = createWriteStream(targetPath);

    return new Promise((resolve, reject) => {
      stringifier.pipe(writeStream);

      let settled = false;
      let closePromise: Promise<void> | undefined;

      const closeFile = (): Promise<void> => {
        if (!closePromise) {
          closePromise = fileHandle.close();
        }
        return closePromise;
      };

      const settleResolve = () => {
        if (settled) {
          return;
        }
        settled = true;
        closeFile()
          .catch(() => {
            // Ignore close failures on resolve path to avoid leaving the promise pending.
          })
          .finally(() => {
            resolve("Done");
          });
      };

      const settleReject = (err: unknown) => {
        if (settled) {
          return;
        }
        settled = true;
        closeFile()
          .catch(() => {
            // Preserve original error when rejecting.
          })
          .finally(() => {
            reject(err);
          });
      };

      writeStream.on("finish", settleResolve);
      writeStream.on("error", settleReject);
      stringifier.on("error", settleReject);

      const writeRow = async (row: Record<string, unknown>) => {
        if (stringifier.write(row)) {
          return;
        }
        await new Promise<void>((drainResolve, drainReject) => {
          stringifier.once("drain", drainResolve);
          stringifier.once("error", drainReject);
        });
      };

      (async () => {
        try {
          let rowStart = 0;
          for (const rowGroup of metadata.row_groups) {
            const numRows = Number(rowGroup.num_rows);
            const rowEnd = rowStart + numRows;

            await parquetRead({
              file: file,
              rowStart,
              rowEnd,
              rowFormat: "object",
              onComplete: async (rows) => {
                if (Array.isArray(rows)) {
                  for (const row of rows) {
                    await writeRow(row);
                  }
                }
              },
            });
            rowStart = rowEnd;
          }
          stringifier.end();
        } catch (err) {
          settleReject(err);
          writeStream.destroy(err as Error);
          stringifier.destroy(err as Error);
        }
      })();
    });
  } catch (err) {
    await fileHandle.close();
    throw err;
  }
}
