using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Lucene.Net.Store.Azure
{
    using System.Globalization;
    using System.Net;
    using System.Net.Http;
    using System.Xml.Linq;
    using System.Xml.XPath;

    /// <summary>
    /// The azure directory.
    /// </summary>
    public class AzureDirectory : Directory
    {
        /// <summary>
        /// The _locks.
        /// </summary>
        private readonly Dictionary<string, AzureLock> locks = new Dictionary<string, AzureLock>();

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureDirectory"/> class. 
        /// </summary>
        /// <param name="accountName">The storage account name.</param>
        /// <param name="accountKey">The storage account key.</param>
        /// <param name="containerName">name of container (folder in blob storage).</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache.</param>
        /// <param name="rootFolder">path of the root folder inside the container.</param>
        public AzureDirectory(
            string accountName,
            string accountKey,
            string containerName = null,
            Directory cacheDirectory = null,
            string rootFolder = null)
        {
            if (string.IsNullOrEmpty(accountName))
            {
                throw new ArgumentNullException("accountName");
            }

            this.ContainerName = string.IsNullOrEmpty(containerName) ? "lucene" : containerName.ToLower();
            this.RootFolder = string.IsNullOrEmpty(rootFolder) ? string.Empty : rootFolder.Trim('/') + "/";

            StorageRestClient.AccountName = accountName;
            StorageRestClient.AccountKey = accountKey;

            this.InitCacheDirectory(cacheDirectory);
        }

        /// <summary>
        /// Gets the blob container name.
        /// </summary>
        public string ContainerName { get; private set; }

        /// <summary>
        /// Gets the root folder.
        /// </summary>
        public string RootFolder { get; private set; }

        /// <summary>
        /// Gets the cache directory.
        /// </summary>
        public Directory CacheDirectory { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether compress blobs.
        /// </summary>
        public bool CompressBlobs { get; set; }

        /// <summary>
        /// Clear the cache.
        /// </summary>
        public void ClearCache()
        {
            foreach (string file in this.CacheDirectory.ListAll())
            {
                this.CacheDirectory.DeleteFile(file);
            }
        }

        /// <summary>
        /// Create a blob container.
        /// </summary>
        public void CreateContainer()
        {
            var existed = false;
            StorageRestClient.Run(
                HttpMethod.Get,
                string.Format(CultureInfo.InvariantCulture, "{0}?restype=container", this.ContainerName),
                null,
                null,
                response => { existed = response.StatusCode == HttpStatusCode.OK; }).Wait();

            if (!existed)
            {
                StorageRestClient.Run(
                    HttpMethod.Put,
                    string.Format(CultureInfo.InvariantCulture, "{0}?restype=container", this.ContainerName),
                    null,
                    null,
                    response =>
                    {
                        if (!response.IsSuccessStatusCode)
                        {
                            throw new Exception("Failed to create new container.");
                        }
                    }).Wait();
            }
        }

        /// <summary>
        /// Returns an array of strings, one for each file in the directory. 
        /// </summary>
        /// <returns>
        /// The file name in string array.
        /// </returns>
        public override string[] ListAll()
        {
            XElement doc = null;
            StorageRestClient.Run(
                HttpMethod.Get,
                string.Format(CultureInfo.InvariantCulture, "{0}?restype=container&comp=list", this.ContainerName),
                null,
                null,
                response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        doc = XDocument.Parse(response.Content.ReadAsStringAsync().Result).Root;
                    }
                }).Wait();

            return doc == null ? new string[0] : doc.XPathSelectElements("./Blobs/Blob/Name").Select(b => b.Value).ToArray();
        }

        /// <summary>
        /// Returns true if a file with the given name exists. 
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool FileExists(string name)
        {
            var existed = false;
            StorageRestClient.Run(
                HttpMethod.Head,
                string.Format(CultureInfo.InvariantCulture, "{0}/{1}", this.ContainerName, name),
                null,
                null,
                response => { existed = response.IsSuccessStatusCode; }).Wait();

            return existed;
        }

        /// <summary>
        /// Returns the time the named file was last modified. 
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="long"/>.
        /// </returns>
        public override long FileModified(string name)
        {
            var time = 0L;
            StorageRestClient.Run(
                HttpMethod.Head,
                string.Format(CultureInfo.InvariantCulture, "{0}/{1}", this.ContainerName, name),
                null,
                null,
                response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        var cachedModifyTime = response.Headers.FirstOrDefault(h => h.Key == StorageRestClient.MetadataHeaderTemplate + "CachedLastModified");
                        if (long.TryParse(cachedModifyTime.Value.FirstOrDefault(), out time))
                        {
                            return;
                        }

                        if (response.Content.Headers.LastModified.HasValue)
                        {
                            time = response.Content.Headers.LastModified.Value.UtcDateTime.ToFileTimeUtc();
                        }
                    }
                }).Wait();

            return time;
        }

        /// <summary>
        /// Removes an existing file in the directory. 
        /// </summary>
        /// <param name="name">The name.</param>
        public override void DeleteFile(string name)
        {
            Debug.WriteLine(string.Format("DELETE {0}/{1}", this.ContainerName, name));

            StorageRestClient.Run(
                HttpMethod.Delete,
                string.Format(CultureInfo.InvariantCulture, "{0}/{1}", this.ContainerName, name),
                null,
                null,
                response =>
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new Exception("Failed to delete a blob.");
                    }
                }).Wait();

            if (this.CacheDirectory.FileExists(name + ".blob"))
            {
                this.CacheDirectory.DeleteFile(name + ".blob");
            }

            if (this.CacheDirectory.FileExists(name))
            {
                this.CacheDirectory.DeleteFile(name);
            }
        }

        /// <summary>
        /// Returns the length of a file in the directory. 
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="long"/>.
        /// </returns>
        public override long FileLength(string name)
        {
            var length = 0L;
            StorageRestClient.Run(
                HttpMethod.Head,
                string.Format(CultureInfo.InvariantCulture, "{0}/{1}", this.ContainerName, name),
                null,
                null,
                response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        var lengthMetadata = response.Headers.FirstOrDefault(h => h.Key == StorageRestClient.MetadataHeaderTemplate + "CachedLength");
                        if (long.TryParse(lengthMetadata.Value.FirstOrDefault(), out length))
                        {
                            return;
                        }

                        if (response.Content.Headers.ContentLength.HasValue)
                        {
                            length = response.Content.Headers.ContentLength.Value;
                        }
                    }
                }).Wait();

            return length; // fall back to actual blob size
        }

        /// <summary>
        /// Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="IndexOutput"/>.
        /// </returns>
        public override IndexOutput CreateOutput(string name)
        {
            return new AzureIndexOutput(this, this.RootFolder + name);
        }

        /// <summary>
        /// Returns a stream reading an existing file. 
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="IndexInput"/>.
        /// </returns>
        public override IndexInput OpenInput(string name)
        {
            try
            {
                var blob = _blobContainer.GetBlockBlobReference(RootFolder + name);
                blob.FetchAttributes();
                return new AzureIndexInput(this, blob);
            }
            catch (Exception err)
            {
                throw new FileNotFoundException(name, err);
            }
        }

        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        /// <returns>The lock.</returns>
        public override Lock MakeLock(string name)
        {
            lock (this.locks)
            {
                if (!this.locks.ContainsKey(name))
                {
                    this.locks.Add(name, new AzureLock(this.RootFolder + name, this));
                }

                return this.locks[name];
            }
        }

        /// <summary>
        /// Clear the lock.
        /// </summary>
        /// <param name="name">The name.</param>
        public override void ClearLock(string name)
        {
            lock (this.locks)
            {
                if (this.locks.ContainsKey(name))
                {
                    this.locks[name].BreakLock();
                }
            }

            this.CacheDirectory.ClearLock(name);
        }

        /// <summary>
        /// Check whether should compress file.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>
        /// True when compression is needed.
        /// </returns>
        public virtual bool ShouldCompressFile(string path)
        {
            if (!this.CompressBlobs)
            {
                return false;
            }

            var ext = Path.GetExtension(path);
            switch (ext)
            {
                case ".cfs":
                case ".fdt":
                case ".fdx":
                case ".frq":
                case ".tis":
                case ".tii":
                case ".nrm":
                case ".tvx":
                case ".tvd":
                case ".tvf":
                case ".prx":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Open cached input as stream.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="StreamInput"/>.
        /// </returns>
        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(this.CacheDirectory.OpenInput(name));
        }

        /// <summary>
        /// Create cached output as stream.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>
        /// The <see cref="StreamOutput"/>.
        /// </returns>
        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(this.CacheDirectory.CreateOutput(name));
        }

        /// <summary>Set the modified time of an existing file to now. </summary>
        /// <param name="name">The name.</param>
        public override void TouchFile(string name)
        {
            this.CacheDirectory.TouchFile(name);
        }

        /// <summary>
        /// Closes the store. 
        /// </summary>
        /// <param name="disposing">The disposing.</param>
        protected override void Dispose(bool disposing)
        {
            foreach (var l in this.locks)
            {
                this.CacheDirectory.ClearLock(l.Key);
            }
        }

        /// <summary>
        /// Initialize the cache directory.
        /// </summary>
        /// <param name="cacheDirectory">The cache directory.</param>
        private void InitCacheDirectory(Directory cacheDirectory)
        {
            if (cacheDirectory != null)
            {
                // save it off
                this.CacheDirectory = cacheDirectory;
            }
            else
            {
                var cachePath = Path.Combine(Path.GetPathRoot(Environment.SystemDirectory), "AzureDirectory");
                var azureDir = new DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                {
                    azureDir.Create();
                }

                var catalogPath = Path.Combine(cachePath, this.ContainerName);
                var catalogDir = new DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                {
                    catalogDir.Create();
                }

                this.CacheDirectory = FSDirectory.Open(catalogPath);
            }

            this.CreateContainer();
        }
    }
}
