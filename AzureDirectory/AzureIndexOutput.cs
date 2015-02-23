using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Net.Http;

    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class AzureIndexOutput : IndexOutput
    {
        /// <summary>
        /// The azure directory.
        /// </summary>
        private readonly AzureDirectory azureDirectory;

        /// <summary>
        /// The _blob.
        /// </summary>
        private readonly string blob;

        /// <summary>
        /// The _file mutex.
        /// </summary>
        private readonly Mutex fileMutex;

        /// <summary>
        /// The index output.
        /// </summary>
        private IndexOutput indexOutput;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureIndexOutput"/> class.
        /// </summary>
        /// <param name="azureDirectory">The azure directory.</param>
        /// <param name="blob">The blob.</param>
        public AzureIndexOutput(AzureDirectory azureDirectory, string blob)
        {
            this.fileMutex = BlobMutexManager.GrabMutex(blob); 
            this.fileMutex.WaitOne();

            try
            {
                this.azureDirectory = azureDirectory;
                this.blob = blob;

                // create the local cache one we will operate against...
                this.indexOutput = this.azureDirectory.CacheDirectory.CreateOutput(blob);
            }
            finally
            {
                this.fileMutex.ReleaseMutex();
            }
        }

        /// <summary>
        /// Gets the cache directory.
        /// </summary>
        public Lucene.Net.Store.Directory CacheDirectory
        {
            get { return this.azureDirectory.CacheDirectory; }
        }

        /// <summary>
        /// Gets the length.
        /// </summary>
        public override long Length
        {
            get
            {
                return this.indexOutput.Length;
            }
        }

        /// <summary>
        /// Gets the file pointer.
        /// </summary>
        public override long FilePointer
        {
            get
            {
                return this.indexOutput.FilePointer;
            }
        }

        /// <summary>
        /// The flush.
        /// </summary>
        public override void Flush()
        {
            this.indexOutput.Flush();
        }

        /// <summary>
        /// Write a byte.
        /// </summary>
        /// <param name="b">The byte.</param>
        public override void WriteByte(byte b)
        {
            this.indexOutput.WriteByte(b);
        }

        /// <summary>
        /// Write bytes.
        /// </summary>
        /// <param name="b">The bytes.</param>
        /// <param name="length">The length.</param>
        public override void WriteBytes(byte[] b, int length)
        {
            this.indexOutput.WriteBytes(b, length);
        }

        /// <summary>
        /// Write bytes.
        /// </summary>
        /// <param name="b">The bytes.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="length">The length.</param>
        public override void WriteBytes(byte[] b, int offset, int length)
        {
            this.indexOutput.WriteBytes(b, offset, length);
        }

        /// <summary>
        /// Seek in the index output.
        /// </summary>
        /// <param name="pos">The position.</param>
        public override void Seek(long pos)
        {
            this.indexOutput.Seek(pos);
        }

        /// <summary>
        /// Dispose the index output.
        /// </summary>
        /// <param name="disposing">The disposing.</param>
        protected override void Dispose(bool disposing)
        {
            this.fileMutex.WaitOne();
            try
            {
                // make sure it's all written out
                this.indexOutput.Flush();

                var originalLength = this.indexOutput.Length;
                this.indexOutput.Dispose();

                Stream blobStream;

                // optionally put a compressor around the blob stream
                if (this.azureDirectory.ShouldCompressFile(this.blob))
                {
                    blobStream = this.CompressStream(this.blob, originalLength);
                }
                else
                {
                    blobStream = new StreamInput(this.azureDirectory.CacheDirectory.OpenInput(this.blob));
                }

                try
                {
                    // push the blobStream up to the cloud
                    StorageRestClient.Run(
                        HttpMethod.Put,
                        string.Format(CultureInfo.InvariantCulture, "{0}/{1}", this.azureDirectory.ContainerName, this.blob),
                        null,
                        Tuple.Create(string.Empty, blobStream),
                        response =>
                        {
                            if (!response.IsSuccessStatusCode)
                            {
                                throw new Exception("Failed to upload blob.");
                            }
                        }).Wait();

                    // set the metadata with the original index file properties
                    StorageRestClient.Run(
                        HttpMethod.Put,
                        string.Format(CultureInfo.InvariantCulture, "{0}/{1}?comp=metadata", this.azureDirectory.ContainerName, this.blob),
                        new Dictionary<string, string>
                        {
                            { StorageRestClient.MetadataHeaderTemplate + "CachedLength", originalLength.ToString(CultureInfo.InvariantCulture) },
                            { StorageRestClient.MetadataHeaderTemplate + "CachedLastModified", this.azureDirectory.CacheDirectory.FileModified(this.blob).ToString(CultureInfo.InvariantCulture) }
                        }, 
                        null,
                        response =>
                        {
                            if (!response.IsSuccessStatusCode)
                            {
                                throw new Exception("Failed to update blob metadata.");
                            }
                        }).Wait();

                    Debug.WriteLine("PUT {1} bytes to {0} in cloud", this.blob, blobStream.Length);
                }
                finally
                {
                    blobStream.Dispose();
                }

                Debug.WriteLine(string.Format("CLOSED WRITESTREAM {0}", this.blob));

                // clean up
                this.indexOutput = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                this.fileMutex.ReleaseMutex();
            }
        }

        /// <summary>
        /// Compress the stream.
        /// </summary>
        /// <param name="fileName">The file name.</param>
        /// <param name="originalLength">The original length.</param>
        /// <returns>
        /// The <see cref="MemoryStream"/>.
        /// </returns>
        private MemoryStream CompressStream(string fileName, long originalLength)
        {
            // unfortunately, deflate stream doesn't allow seek, and we need a seekable stream
            // to pass to the blob storage stuff, so we compress into a memory stream
            var compressedStream = new MemoryStream();

            try
            {
                using (var indexInput = this.azureDirectory.CacheDirectory.OpenInput(fileName))
                using (var compressor = new DeflateStream(compressedStream, CompressionMode.Compress, true))
                {
                    // compress to compressedOutputStream
                    var bytes = new byte[indexInput.Length()];
                    indexInput.ReadBytes(bytes, 0, bytes.Length);
                    compressor.Write(bytes, 0, bytes.Length);
                }

                // seek back to beginning of comrpessed stream
                compressedStream.Seek(0, SeekOrigin.Begin);

                Debug.WriteLine(
                    "COMPRESSED {0} -> {1} {2}% to {3}",
                   originalLength,
                   compressedStream.Length,
                   ((float)compressedStream.Length / (float)originalLength) * 100,
                   this.blob);
            }
            catch
            {
                // release the compressed stream resources if an error occurs
                compressedStream.Dispose();
                throw;
            }

            return compressedStream;
        }
    }
}
