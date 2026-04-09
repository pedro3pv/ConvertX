import { expect, test, describe, mock } from "bun:test";
import { Writable } from "node:stream";

// Helper mocks
const mockParquetMetadataAsync = mock(async () => {
  return {
    row_groups: [{ num_rows: 1 }]
  };
});

const mockParquetRead = mock(async (options: any) => {
  if (options.onComplete) {
    options.onComplete([{ col1: "value" }]);
  }
  return [];
});

const mockCreateWriteStream = mock((_path: string) => {
  return new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    }
  });
});

// Mocking node:fs/promises for fileHandle.read
const mockFileHandle = {
  stat: mock(async () => ({ size: 100 })),
  read: mock(async (buf: Buffer) => {
    return { bytesRead: buf.length, buffer: buf };
  }),
  close: mock(async () => {})
};

mock.module("node:fs/promises", () => ({
  open: mock(async () => mockFileHandle)
}));

mock.module("hyparquet", () => {
  return {
    parquetMetadataAsync: mockParquetMetadataAsync,
    parquetRead: mockParquetRead
  };
});


mock.module("node:fs", () => {
  return {
    createWriteStream: mockCreateWriteStream
  };
});

// Import after mocking
import { convert } from "../../src/converters/parquet";

describe("parquet converter", () => {
  test("convert resolves when process succeeds", async () => {
    const result = await convert("input.parquet", "parquet", "csv", "output.csv");
    expect(result).toBe("Done");
  });

  test("convert rejects when metadata fails", async () => {
    mockParquetMetadataAsync.mockRejectedValueOnce(new Error("Metadata error"));
    
    expect(
      convert("invalid.parquet", "parquet", "csv", "output.csv")
    ).rejects.toThrow("Metadata error");
  });

});
