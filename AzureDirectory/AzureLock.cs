using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;

    /// <summary>
    /// Implements lock semantics on AzureDirectory via a blob lease
    /// </summary>
    public class AzureLock : Lock, IDisposable
    {
        /// <summary>
        /// The _lock file.
        /// </summary>
        private readonly string lockFile;

        /// <summary>
        /// The _azure directory.
        /// </summary>
        private readonly AzureDirectory azureDirectory;

        /// <summary>
        /// The lease id.
        /// </summary>
        private string leaseId;

        /// <summary>
        /// The renew timer.
        /// </summary>
        private Timer renewTimer;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureLock"/> class.
        /// </summary>
        /// <param name="lockFile">
        /// The lock file.
        /// </param>
        /// <param name="directory">
        /// The directory.
        /// </param>
        public AzureLock(string lockFile, AzureDirectory directory)
        {
            this.lockFile = lockFile;
            this.azureDirectory = directory;
        }

        #region Lock methods

        /// <summary>
        /// Check if the azure lock is locked.
        /// </summary>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool IsLocked()
        {
            var statusCode = HttpStatusCode.OK;

            if (!string.IsNullOrEmpty(this.leaseId))
            {
                Debug.Print("IsLocked() : {0}", this.leaseId);
                return string.IsNullOrEmpty(this.leaseId);
            }

            do
            {
                var tempLease = string.Empty;
                StorageRestClient.Run(
                    HttpMethod.Put,
                    string.Format("{0}/{1}?comp=lease", this.azureDirectory.ContainerName, this.lockFile),
                    new Dictionary<string, string>
                        {
                            { "x-ms-lease-action", "acquire" },
                            { "x-ms-lease-duration", "60" },
                            { "x-ms-proposed-lease-id", this.leaseId }
                        },
                    null,
                    response =>
                    {
                        statusCode = response.StatusCode;
                        if (response.IsSuccessStatusCode)
                        {
                            tempLease = response.Headers.GetValues("x-ms-lease-id").FirstOrDefault();
                        }
                    }).Wait();

                if (string.IsNullOrEmpty(tempLease))
                {
                    Debug.Print("IsLocked() : TRUE");
                    return true;
                }

                StorageRestClient.Run(
                    HttpMethod.Put,
                    string.Format("{0}/{1}?comp=lease", this.azureDirectory.ContainerName, this.lockFile),
                    new Dictionary<string, string>
                        {
                            { "x-ms-lease-action", "release" },
                            { "x-ms-lease-id", tempLease }
                        },
                    null,
                    response =>
                    {
                        statusCode = response.StatusCode;
                    }).Wait();
            }
            while (statusCode != HttpStatusCode.OK && this.HandleWebException(statusCode));

            this.leaseId = null;
            return false;
        }

        /// <summary>
        /// The obtain.
        /// </summary>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool Obtain()
        {
            Debug.Print("AzureLock:Obtain({0}) : {1}", this.lockFile, this.leaseId);

            if (!string.IsNullOrEmpty(this.leaseId))
            {
                return true;
            }

            var statusCode = HttpStatusCode.OK;
            do
            {
                StorageRestClient.Run(
                    HttpMethod.Put,
                    string.Format("{0}/{1}?comp=lease", this.azureDirectory.ContainerName, this.lockFile),
                    new Dictionary<string, string>
                    {
                        { "x-ms-lease-action", "acquire" },
                        { "x-ms-lease-duration", "60" },
                        { "x-ms-proposed-lease-id", this.leaseId }
                    },
                    null,
                    response =>
                    {
                        statusCode = response.StatusCode;
                        if (response.IsSuccessStatusCode)
                        {
                            this.leaseId = response.Headers.GetValues("x-ms-lease-id").FirstOrDefault();
                        }
                    }).Wait();

                Debug.Print("AzureLock:Obtain({0}): AcquireLease : {1}", this.lockFile, this.leaseId);
            }
            while (statusCode != HttpStatusCode.OK && this.HandleWebException(statusCode));

            // keep the lease alive by renewing every 30 seconds
            var interval = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
            this.renewTimer = new Timer(
                obj =>
                {
                    try
                    {
                        var al = (AzureLock)obj;
                        al.Renew();
                    }
                    catch (Exception err)
                    {
                        Debug.Print(err.ToString());
                    }
                },
                this,
                interval,
                interval);

            return !string.IsNullOrEmpty(this.leaseId);
        }

        /// <summary>
        /// Renew the lock.
        /// </summary>
        public void Renew()
        {
            if (string.IsNullOrEmpty(this.leaseId))
            {
                return;
            }

            Debug.Print("AzureLock:Renew({0} : {1}", this.lockFile, this.leaseId);

            StorageRestClient.Run(
                HttpMethod.Put,
                string.Format("{0}/{1}?comp=lease", this.azureDirectory.ContainerName, this.lockFile),
                new Dictionary<string, string>
                    {
                        { "x-ms-lease-action", "renew" },
                        { "x-ms-lease-id", this.leaseId }
                    },
                null,
                response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        throw new Exception("Failed to rebnew the blob lease.");
                    }
                }).Wait();
        }

        /// <summary>
        /// Release the lock.
        /// </summary>
        public override void Release()
        {
            Debug.Print("AzureLock:Release({0}) {1}", this.lockFile, this.leaseId);

            if (string.IsNullOrEmpty(this.leaseId))
            {
                return;
            }

            StorageRestClient.Run(
                HttpMethod.Put,
                string.Format("{0}/{1}?comp=lease", this.azureDirectory.ContainerName, this.lockFile),
                new Dictionary<string, string>
                    {
                        { "x-ms-lease-action", "release" },
                        { "x-ms-lease-id", this.leaseId }
                    },
                null,
                response =>
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new Exception("Failed to release the blob lease.");
                    }
                }).Wait();

            if (this.renewTimer != null)
            {
                this.renewTimer.Dispose();
                this.renewTimer = null;
            }

            this.leaseId = null;
        }

        #endregion

        /// <summary>
        /// Break the lock.
        /// </summary>
        public void BreakLock()
        {
            Debug.Print("AzureLock:BreakLock({0}) {1}", this.lockFile, this.leaseId);

            if (string.IsNullOrEmpty(this.leaseId))
            {
                return;
            }

            StorageRestClient.Run(
                HttpMethod.Put,
                string.Format("{0}/{1}?comp=lease", this.azureDirectory.ContainerName, this.lockFile),
                new Dictionary<string, string>
                {
                    { "x-ms-lease-action", "break" },
                    { "x-ms-lease-id", this.leaseId }
                },
                null,
                response =>
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        Trace.TraceError("Failed to break the blob lease.");
                    }
                }).Wait();

            this.leaseId = null;
        }

        /// <summary>
        /// The to string.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public override string ToString()
        {
            return string.Format("AzureLock@{0}.{1}", this.lockFile, this.leaseId);
        }

        /// <summary>
        /// The dispose.
        /// </summary>
        public void Dispose()
        {
            if (this.renewTimer == null)
            {
                return;
            }

            this.renewTimer.Dispose();
            this.renewTimer = null;
        }

        /// <summary>
        /// The handle web exception.
        /// </summary>
        /// <param name="statusCode">The status code.</param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        private bool HandleWebException(HttpStatusCode statusCode)
        {
            if (statusCode != HttpStatusCode.NotFound && statusCode != HttpStatusCode.Conflict)
            {
                return false;
            }

            this.azureDirectory.CreateContainer();

            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(this.lockFile)))
            {
                StorageRestClient.Run(
                    HttpMethod.Put,
                    string.Format(CultureInfo.InvariantCulture, "{0}/{1}", this.azureDirectory.ContainerName, this.lockFile),
                    null,
                    Tuple.Create(string.Empty, (Stream)stream),
                    response =>
                    {
                        if (!response.IsSuccessStatusCode)
                        {
                            throw new Exception("Failed to upload lock file.");
                        }
                    }).Wait();
            }

            return true;
        }
    }
}
