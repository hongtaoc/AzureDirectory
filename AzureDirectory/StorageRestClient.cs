namespace Lucene.Net.Store.Azure
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web;

    /// <summary>
    /// The storage rest client.
    /// </summary>
    public static class StorageRestClient
    {
        /// <summary>
        /// The metadata header template.
        /// </summary>
        public const string MetadataHeaderTemplate = "x-ms-meta-";

        /// <summary>
        /// Initializes static members of the <see cref="StorageRestClient"/> class.
        /// </summary>
        static StorageRestClient()
        {
            Version = "2014-02-14";
        }

        /// <summary>
        /// Gets or sets the account name.
        /// </summary>
        public static string AccountName { get; set; }

        /// <summary>
        /// Gets or sets the account key.
        /// </summary>
        public static string AccountKey { get; set; }

        /// <summary>
        /// Gets or sets the version.
        /// </summary>
        public static string Version { get; set; }

        /// <summary>
        /// Run a Azure REST HTTP method.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <param name="address">The address.</param>
        /// <param name="headers">The request headers.</param>
        /// <param name="content">The content type and stream tuple.</param>
        /// <param name="action">The action.</param>
        /// <returns>The async <see cref="Task"/>.</returns>
        public static async Task Run(HttpMethod method, string address, IDictionary<string, string> headers, Tuple<string, Stream> content, Action<HttpResponseMessage> action)
        {
            var length = content == null ? 0 : content.Item2.Length;

            // For GET and HEAD, set the lenght to -1 to exclude it in the request header hash.
            length = new[] { HttpMethod.Get, HttpMethod.Head }.Contains(method) ? -1 : length;

            if (headers == null)
            {
                headers = new Dictionary<string, string>();
            }

            SetAzureRequestHeader(headers, method.Method, address, length);

            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(string.Format("https://{0}.blob.core.windows.net/", AccountName));

                foreach (var header in headers)
                {
                    client.DefaultRequestHeaders.TryAddWithoutValidation(header.Key, header.Value);
                }

                using (var request = new HttpRequestMessage(method, address))
                {
                    if (content != null)
                    {
                        using (var streamContent = new StreamContent(content.Item2))
                        {
                            request.Content = streamContent;
                            if (!string.IsNullOrEmpty(content.Item1))
                            {
                                request.Content.Headers.ContentType = new MediaTypeHeaderValue(content.Item1);
                            }

                            using (var response = await client.SendAsync(request, CancellationToken.None))
                            {
                                action(response);
                            }
                        }
                    }
                    else
                    {
                        using (var response = await client.SendAsync(request, CancellationToken.None))
                        {
                            action(response);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Set blob authorization header azure request header.
        /// </summary>
        /// <param name="requestHeaders">The request headers.</param>
        /// <param name="httpMethod">The http Method.</param>
        /// <param name="address">The url path.</param>
        /// <param name="contentLength">The content length.</param>
        private static void SetAzureRequestHeader(IDictionary<string, string> requestHeaders, string httpMethod, string address, long contentLength)
        {
            requestHeaders["x-ms-date"] = DateTime.UtcNow.ToString("R");
            requestHeaders["x-ms-version"] = Version;

            if (AccountKey == "UseDevelopmentStorage=true")
            {
                return;
            }

            var canonicalizedHeaders = string.Join("\n", requestHeaders.OrderBy(k => k.Key).Select(p => string.Format(CultureInfo.InvariantCulture, "{0}:{1}", p.Key.ToLower(), p.Value)).ToArray());

            var uri = new Uri(new Uri(string.Format("https://{0}.blob.core.windows.net/", AccountName)), address);
            var canonicalizedResource = string.Format(CultureInfo.InvariantCulture, "/{0}{1}", AccountName, uri.AbsolutePath);
            var queryStringValues = HttpUtility.ParseQueryString(uri.Query);
            if (queryStringValues.Count > 0)
            {
                canonicalizedResource = queryStringValues.AllKeys.OrderBy(k => k).Aggregate(canonicalizedResource, (current, key) => current + string.Format(CultureInfo.InvariantCulture, "\n{0}:{1}", key, queryStringValues[key]));
            }

            var stringToSign = string.Format(CultureInfo.InvariantCulture, "{0}\n\n\n{1}\n\n\n\n\n\n\n\n\n{2}\n{3}", httpMethod, contentLength >= 0 ? contentLength.ToString(CultureInfo.InvariantCulture) : string.Empty, canonicalizedHeaders, canonicalizedResource);

            requestHeaders["Authorization"] = GenerateAuthorizationHeaderValue(stringToSign);
        }

        /// <summary>
        /// Create blob authorization header value.
        /// </summary>
        /// <param name="canonicalizedString">The canonicalized string.</param>
        /// <returns>
        /// The authorization header.
        /// </returns>
        private static string GenerateAuthorizationHeaderValue(string canonicalizedString)
        {
            string signature;

            using (var hmacSha256 = new HMACSHA256(Convert.FromBase64String(AccountKey)))
            {
                var dataToHmac = Encoding.UTF8.GetBytes(canonicalizedString);
                signature = Convert.ToBase64String(hmacSha256.ComputeHash(dataToHmac));
            }

            return string.Format(CultureInfo.InvariantCulture, "SharedKey {0}:{1}", AccountName, signature);
        }
    }
}
