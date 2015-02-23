using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    using System.Net.Http;

    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput
    {
        /// <summary>
        /// The blob.
        /// </summary>
        private readonly string blob;

        /// <summary>
        /// The _file mutex.
        /// </summary>
        private readonly Mutex fileMutex;

        /// <summary>
        /// The _azure directory.
        /// </summary>
        private AzureDirectory azureDirectory;

        /// <summary>
        /// The index input.
        /// </summary>
        private IndexInput indexInput;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureIndexInput"/> class.
        /// </summary>
        /// <param name="cloneInput">The clone input.</param>
        public AzureIndexInput(AzureIndexInput cloneInput)
        {
            this.fileMutex = BlobMutexManager.GrabMutex(cloneInput.blob);
            this.fileMutex.WaitOne();

            try
            {
                Debug.WriteLine(string.Format("Creating clone for {0}", cloneInput.blob));

                this.azureDirectory = cloneInput.azureDirectory;
                this.blob = cloneInput.blob;
                this.indexInput = cloneInput.indexInput.Clone() as IndexInput;
            }
            catch (Exception)
            {
                // sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet
                // but this covers our tail until I do
                Debug.WriteLine(string.Format("Dagnabbit, falling back to memory clone for {0}", cloneInput.blob));
            }
            finally
            {
                this.fileMutex.ReleaseMutex();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureIndexInput"/> class.
        /// </summary>
        /// <param name="azureDirectory">The azure directory.</param>
        /// <param name="blob">The blob.</param>
        public AzureIndexInput(AzureDirectory azureDirectory, string blob)
        {
            Debug.WriteLine(string.Format("opening {0} ", blob));

            this.fileMutex = BlobMutexManager.GrabMutex(blob);
            this.fileMutex.WaitOne();
            try
            {
                this.azureDirectory = azureDirectory;
                this.blob = blob;

                var fileNeeded = !this.azureDirectory.CacheDirectory.FileExists(blob);
                if (fileNeeded)
                {
                    var cachedLength = this.azureDirectory.CacheDirectory.FileLength(blob);
                    var blobLength = this.azureDirectory.FileLength(blob);

                    fileNeeded = cachedLength != blobLength;
                    if (fileNeeded)
                    {
                        // cachedLastModifiedUTC was not ouputting with a date (just time) and the time was always off
                        var unixDate = this.azureDirectory.CacheDirectory.FileModified(blob);
                        var start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        var cachedLastModifiedUtc = start.AddMilliseconds(unixDate).ToUniversalTime();

                        var blobLastModifiedUtc = new DateTime(this.azureDirectory.FileModified(blob)).ToUniversalTime();

                        fileNeeded = cachedLastModifiedUtc != blobLastModifiedUtc;
                        if (fileNeeded)
                        {
                            var timeSpan = blobLastModifiedUtc.Subtract(cachedLastModifiedUtc);
                            fileNeeded = timeSpan.TotalSeconds > 1;
                            if (!fileNeeded)
                            {
                                // file not needed
                                Debug.WriteLine(timeSpan.TotalSeconds);
                            }
                        }
                    }
                }

                // if the file does not exist
                // or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
                if (fileNeeded)
                {
                    if (this.azureDirectory.ShouldCompressFile(blob))
                    {
                        this.InflateStream(blob);
                    }
                    else
                    {
                        using (var fileStream = this.azureDirectory.CreateCachedOutputAsStream(blob))
                        {
                            // get the blob
                            StorageRestClient.Run(
                                HttpMethod.Get,
                                string.Format("{0}/{1}", this.azureDirectory.ContainerName, blob),
                                null,
                                null,
                                response =>
                                {
                                    if (!response.IsSuccessStatusCode || response.Content == null)
                                    {
                                        return;
                                    }

                                    using (var azureStream = response.Content.ReadAsStreamAsync().Result)
                                    {
                                        azureStream.CopyTo(fileStream);
                                    }
                                }).Wait();

                            fileStream.Flush();
                            Debug.WriteLine("GET {0} RETREIVED {1} bytes", blob, fileStream.Length);
                        }
                    }
                }
                else
                {
                    Debug.WriteLine(string.Format("Using cached file for {0}", blob));
                }

                // and open it as an input 
                this.indexInput = this.azureDirectory.CacheDirectory.OpenInput(blob);
            }
            finally
            {
                this.fileMutex.ReleaseMutex();
            }
        }

        /// <summary>
        /// Gets the file pointer.
        /// </summary>
        public override long FilePointer
        {
            get
            {
                return this.indexInput.FilePointer;
            }
        }

        /// <summary>
        /// Read a byte.
        /// </summary>
        /// <returns>
        /// The <see cref="byte"/>.
        /// </returns>
        public override byte ReadByte()
        {
            return this.indexInput.ReadByte();
        }

        /// <summary>
        /// Read bytes.
        /// </summary>
        /// <param name="b">
        /// The bytes.
        /// </param>
        /// <param name="offset">
        /// The offset.
        /// </param>
        /// <param name="len">
        /// The length.
        /// </param>
        public override void ReadBytes(byte[] b, int offset, int len)
        {
            this.indexInput.ReadBytes(b, offset, len);
        }

        /// <summary>
        /// Seek in the index input.
        /// </summary>
        /// <param name="pos">The position.</param>
        public override void Seek(long pos)
        {
            this.indexInput.Seek(pos);
        }

        /// <summary>
        /// Get the index input length.
        /// </summary>
        /// <returns>
        /// The <see cref="long"/>.
        /// </returns>
        public override long Length()
        {
            return this.indexInput.Length();
        }

        /// <summary>
        /// Clone the index input.
        /// </summary>
        /// <returns>
        /// The <see cref="object"/>.
        /// </returns>
        public override object Clone()
        {
            IndexInput clone = null;
            try
            {
                this.fileMutex.WaitOne();
                var input = new AzureIndexInput(this);
                clone = input;
            }
            catch (Exception err)
            {
                Debug.WriteLine(err.ToString());
            }
            finally
            {
                this.fileMutex.ReleaseMutex();
            }

            Debug.Assert(clone != null, "Cannot clone null reference.");
            return clone;
        }

        /// <summary>
        /// Dispose the index input.
        /// </summary>
        /// <param name="disposing">
        /// The disposing.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            this.fileMutex.WaitOne();
            try
            {
                Debug.WriteLine(string.Format("CLOSED READSTREAM local {0}", this.blob));

                this.indexInput.Dispose();
                this.indexInput = null;
                this.azureDirectory = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                this.fileMutex.ReleaseMutex();
            }
        }

        /// <summary>
        /// The inflate stream.
        /// </summary>
        /// <param name="fileName">The file name.</param>
        private void InflateStream(string fileName)
        {
            // then we will get it fresh into local deflatedName 
            // StreamOutput deflatedStream = new StreamOutput(CacheDirectory.CreateOutput(deflatedName));
            using (var deflatedStream = new MemoryStream())
            {
                // get the deflated blob
                StorageRestClient.Run(
                    HttpMethod.Get,
                    string.Format("{0}/{1}", this.azureDirectory.ContainerName, this.blob),
                    null,
                    null,
                    response =>
                        {
                            if (!response.IsSuccessStatusCode || response.Content == null)
                            {
                                return;
                            }

                            using (var azureStream = response.Content.ReadAsStreamAsync().Result)
                            {
                                azureStream.CopyTo(deflatedStream);
                            }
                        }).Wait();

                Debug.WriteLine("GET {0} RETREIVED {1} bytes", this.blob, deflatedStream.Length);

                // seek back to begininng
                deflatedStream.Seek(0, SeekOrigin.Begin);

                // open output file for uncompressed contents
                using (var fileStream = this.azureDirectory.CreateCachedOutputAsStream(fileName))
                using (var decompressor = new DeflateStream(deflatedStream, CompressionMode.Decompress))
                {
                    var bytes = new byte[65535];
                    int read;
                    do
                    {
                        read = decompressor.Read(bytes, 0, 65535);
                        if (read > 0)
                        {
                            fileStream.Write(bytes, 0, read);
                        }
                    }
                    while (read == 65535);
                }
            }
        }
    }
}
