using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using Microsoft.Extensions.Caching.Memory;

using Newtonsoft.Json;

using NuClear.VStore.DataContract;
using NuClear.VStore.Descriptors.Templates;
using NuClear.VStore.Json;
using NuClear.VStore.Locks;
using NuClear.VStore.Options;
using NuClear.VStore.S3;

namespace NuClear.VStore.Templates
{
    public sealed class TemplatesStorageReader
    {
        private readonly IS3Client _s3Client;
        private readonly IMemoryCache _memoryCache;
        private readonly DistributedLockManager _distributedLockManager;
        private readonly string _bucketName;
        private readonly int _degreeOfParallelism;

        public TemplatesStorageReader(CephOptions cephOptions, IS3Client s3Client, IMemoryCache memoryCache, DistributedLockManager distributedLockManager)
        {
            _s3Client = s3Client;
            _memoryCache = memoryCache;
            _distributedLockManager = distributedLockManager;
            _bucketName = cephOptions.TemplatesBucketName;
            _degreeOfParallelism = cephOptions.DegreeOfParallelism;
        }

        public async Task<ContinuationContainer<IdentifyableObjectRecord<long>>> List(string continuationToken)
        {
            var listResponse = await _s3Client.ListObjectsAsync(new ListObjectsRequest { BucketName = _bucketName, Marker = continuationToken });

            var records = listResponse.S3Objects.Select(x => new IdentifyableObjectRecord<long>(long.Parse(x.Key), x.LastModified)).ToList();
            return new ContinuationContainer<IdentifyableObjectRecord<long>>(records, listResponse.NextMarker);
        }

        public async Task<IReadOnlyCollection<ObjectMetadataRecord>> GetTemplateMetadatas(IReadOnlyCollection<long> ids)
        {
            var uniqueIds = new HashSet<long>(ids);
            var partitioner = Partitioner.Create(uniqueIds);
            var result = new ObjectMetadataRecord[uniqueIds.Count];
            var tasks = partitioner
                .GetOrderablePartitions(_degreeOfParallelism)
                .Select(async x =>
                            {
                                while (x.MoveNext())
                                {
                                    var templateId = x.Current.Value;
                                    ObjectMetadataRecord record;
                                    try
                                    {
                                        var versionId = await GetTemplateLatestVersion(templateId);

                                        var response = await _s3Client.GetObjectMetadataAsync(_bucketName, templateId.ToString(), versionId);
                                        var metadataWrapper = MetadataCollectionWrapper.For(response.Metadata);
                                        var author = metadataWrapper.Read<string>(MetadataElement.Author);
                                        var authorLogin = metadataWrapper.Read<string>(MetadataElement.AuthorLogin);
                                        var authorName = metadataWrapper.Read<string>(MetadataElement.AuthorName);

                                        record = new ObjectMetadataRecord(
                                            templateId,
                                            versionId,
                                            response.LastModified,
                                            new AuthorInfo(author, authorLogin, authorName));
                                    }
                                    catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                                    {
                                        record = null;
                                    }
                                    catch (ObjectNotFoundException)
                                    {
                                        record = null;
                                    }

                                    result[x.Current.Key] = record;
                                }
                            });
            await Task.WhenAll(tasks);
            return result.Where(x => x != null).ToList();
        }

        public async Task<TemplateDescriptor> GetTemplateDescriptor(long id, string versionId)
        {
            var objectVersionId = string.IsNullOrEmpty(versionId) ? await GetTemplateLatestVersion(id) : versionId;

            var templateDescriptor = await _memoryCache.GetOrCreateAsync(
                id.AsCacheEntryKey(objectVersionId),
                async entry =>
                    {
                        try
                        {
                            using (var response = await _s3Client.GetObjectAsync(_bucketName, id.ToString(), objectVersionId))
                            {
                                var metadataWrapper = MetadataCollectionWrapper.For(response.Metadata);
                                var author = metadataWrapper.Read<string>(MetadataElement.Author);
                                var authorLogin = metadataWrapper.Read<string>(MetadataElement.AuthorLogin);
                                var authorName = metadataWrapper.Read<string>(MetadataElement.AuthorName);

                                string json;
                                using (var reader = new StreamReader(response.ResponseStream, Encoding.UTF8))
                                {
                                    json = reader.ReadToEnd();
                                }

                                var descriptor = new TemplateDescriptor
                                    {
                                        Id = id,
                                        VersionId = objectVersionId,
                                        LastModified = response.LastModified,
                                        Author = author,
                                        AuthorLogin = authorLogin,
                                        AuthorName = authorName
                                    };
                                JsonConvert.PopulateObject(json, descriptor, SerializerSettings.Default);

                                entry.SetValue(descriptor)
                                     .SetPriority(CacheItemPriority.NeverRemove);

                                return descriptor;
                            }
                        }
                        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                        {
                            throw new ObjectNotFoundException($"Template '{id}' version '{objectVersionId}' not found");
                        }
                    });

            return templateDescriptor;
        }

        public async Task<string> GetTemplateLatestVersion(long id)
        {
            var idAsString = id.ToString();
            var versionsResponse = await _s3Client.ListVersionsAsync(_bucketName, idAsString);
            var version = versionsResponse.Versions.Find(x => x.Key == idAsString && !x.IsDeleteMarker && x.IsLatest);
            if (version == null)
            {
                throw new ObjectNotFoundException($"Template '{id}' versions not found");
            }

            return version.VersionId;
        }

        public async Task<bool> IsTemplateExists(long id)
        {
            var idAsString = id.ToString();
            var listResponse = await _s3Client.ListObjectsV2Async(
                                   new ListObjectsV2Request
                                       {
                                           BucketName = _bucketName,
                                           Prefix = idAsString
                                       });
            return listResponse.S3Objects.FindIndex(o => o.Key == idAsString) != -1;
        }

        public async Task<IReadOnlyCollection<TemplateVersionRecord>> GetTemplateVersions(long id, string initialVersionId)
        {
            var idAsString = id.ToString();
            var templateDescriptors = new List<TemplateDescriptor>();

            async Task<(bool IsTruncated, int NextVersionIndex, string NextVersionIdMarker)> ListVersions(int nextVersionIndex, string nextVersionIdMarker)
            {
                await _distributedLockManager.EnsureLockNotExistsAsync(id);

                var response = await _s3Client.ListVersionsAsync(
                                   new ListVersionsRequest
                                       {
                                           BucketName = _bucketName,
                                           Prefix = idAsString,
                                           VersionIdMarker = nextVersionIdMarker
                                       });

                var nonDeletedVersions = response.Versions.FindAll(x => !x.IsDeleteMarker && x.Key == idAsString);
                nextVersionIndex += nonDeletedVersions.Count;

                var initialVersionIdReached = false;
                var versionInfos = nonDeletedVersions
                    .Aggregate(
                        new List<(string VersionId, DateTime LastModified)>(),
                        (list, next) =>
                            {
                                initialVersionIdReached = initialVersionIdReached ||
                                                          !string.IsNullOrEmpty(initialVersionId) &&
                                                          initialVersionId.Equals(next.VersionId, StringComparison.OrdinalIgnoreCase);
                                if (!initialVersionIdReached)
                                {
                                    list.Add((next.VersionId, next.LastModified));
                                }

                                return list;
                            });

                var descriptors = new TemplateDescriptor[versionInfos.Count];
                var partitioner = Partitioner.Create(versionInfos);
                var tasks = partitioner.GetOrderablePartitions(_degreeOfParallelism)
                                       .Select(async partition =>
                                                   {
                                                       while (partition.MoveNext())
                                                       {
                                                           var index = partition.Current.Key;
                                                           var (versionId, _) = partition.Current.Value;

                                                           descriptors[index] = await GetTemplateDescriptor(id, versionId);
                                                       }
                                                   });
                await Task.WhenAll(tasks);

                templateDescriptors.AddRange(descriptors);

                return (!initialVersionIdReached && response.IsTruncated, nextVersionIndex, response.NextVersionIdMarker);
            }

            var result = await ListVersions(0, null);
            if (templateDescriptors.Count == 0)
            {
                if (!await IsTemplateExists(id))
                {
                    throw new ObjectNotFoundException($"Template '{id}' not found.");
                }

                return Array.Empty<TemplateVersionRecord>();
            }

            while (result.IsTruncated)
            {
                result = await ListVersions(result.NextVersionIndex, result.NextVersionIdMarker);
            }

            var maxVersionIndex = result.NextVersionIndex;
            var records = new TemplateVersionRecord[templateDescriptors.Count];
            for (var index = 0; index < templateDescriptors.Count; ++index)
            {
                var descriptor = templateDescriptors[index];
                records[index] = new TemplateVersionRecord(
                    descriptor.Id,
                    descriptor.VersionId,
                    --maxVersionIndex,
                    descriptor.LastModified,
                    new AuthorInfo(descriptor.Author, descriptor.AuthorLogin, descriptor.AuthorName),
                    descriptor.Properties,
                    descriptor.Elements.Select(x => x.TemplateCode).ToList());
            }

            return records;
        }
    }
}
