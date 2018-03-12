using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using Newtonsoft.Json;

using NuClear.VStore.DataContract;
using NuClear.VStore.Descriptors;
using NuClear.VStore.Descriptors.Objects;
using NuClear.VStore.Descriptors.Objects.Persistence;
using NuClear.VStore.Descriptors.Templates;
using NuClear.VStore.Json;
using NuClear.VStore.Locks;
using NuClear.VStore.Options;
using NuClear.VStore.S3;
using NuClear.VStore.Templates;

namespace NuClear.VStore.Objects
{
    public sealed class ObjectsStorageReader
    {
        private readonly CdnOptions _cdnOptions;
        private readonly IS3Client _s3Client;
        private readonly TemplatesStorageReader _templatesStorageReader;
        private readonly DistributedLockManager _distributedLockManager;
        private readonly string _bucketName;
        private readonly int _degreeOfParallelism;

        public ObjectsStorageReader(
            CephOptions cephOptions,
            CdnOptions cdnOptions,
            IS3Client s3Client,
            TemplatesStorageReader templatesStorageReader,
            DistributedLockManager distributedLockManager)
        {
            _cdnOptions = cdnOptions;
            _s3Client = s3Client;
            _templatesStorageReader = templatesStorageReader;
            _distributedLockManager = distributedLockManager;
            _bucketName = cephOptions.ObjectsBucketName;
            _degreeOfParallelism = cephOptions.DegreeOfParallelism;
        }

        public async Task<ContinuationContainer<IdentifyableObjectRecord<long>>> List(string continuationToken)
        {
            var listResponse = await _s3Client.ListObjectsAsync(new ListObjectsRequest { BucketName = _bucketName, Marker = continuationToken });

            var records = listResponse.S3Objects.Select(x => new IdentifyableObjectRecord<long>(x.Key.AsRootObjectId(), x.LastModified)).Distinct().ToList();
            return new ContinuationContainer<IdentifyableObjectRecord<long>>(records, listResponse.NextMarker);
        }

        public async Task<IReadOnlyCollection<ObjectMetadataRecord>> GetObjectMetadatas(IReadOnlyCollection<long> ids)
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
                                    var id = x.Current.Value;
                                    ObjectMetadataRecord record;
                                    try
                                    {
                                        var objectVersions = await GetObjectLatestVersions(id);
                                        var versionId = objectVersions.Where(v => v.Id.EndsWith(Tokens.ObjectPostfix))
                                                                      .Select(v => v.VersionId)
                                                                      .SingleOrDefault();
                                        if (versionId == null)
                                        {
                                            record = null;
                                        }
                                        else
                                        {
                                            var response = await _s3Client.GetObjectMetadataAsync(_bucketName, id.AsS3ObjectKey(Tokens.ObjectPostfix), versionId);
                                            var metadataWrapper = MetadataCollectionWrapper.For(response.Metadata);
                                            var author = metadataWrapper.Read<string>(MetadataElement.Author);
                                            var authorLogin = metadataWrapper.Read<string>(MetadataElement.AuthorLogin);
                                            var authorName = metadataWrapper.Read<string>(MetadataElement.AuthorName);

                                            record = new ObjectMetadataRecord(
                                                id,
                                                versionId,
                                                response.LastModified,
                                                new AuthorInfo(author, authorLogin, authorName));
                                        }
                                    }
                                    catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                                    {
                                        record = null;
                                    }

                                    result[x.Current.Key] = record;
                                }
                            });
            await Task.WhenAll(tasks);
            return result.Where(x => x != null).ToList();
        }

        public async Task<IVersionedTemplateDescriptor> GetTemplateDescriptor(long id, string versionId)
        {
            var (persistenceDescriptor, _, _, _) =
                await GetObjectFromS3<ObjectPersistenceDescriptor>(id.AsS3ObjectKey(Tokens.ObjectPostfix), versionId, default);
            return await _templatesStorageReader.GetTemplateDescriptor(persistenceDescriptor.TemplateId, persistenceDescriptor.TemplateVersionId);
        }

        public async Task<IReadOnlyCollection<ObjectVersionRecord>> GetObjectVersions(long id, string initialVersionId)
        {
            var objectDescriptors = new List<ObjectDescriptor>();

            async Task<(bool IsTruncated, int NextVersionIndex, string NextVersionIdMarker)> ListVersions(int nextVersionIndex, string nextVersionIdMarker)
            {
                await _distributedLockManager.EnsureLockNotExistsAsync(id);

                var response = await _s3Client.ListVersionsAsync(
                                   new ListVersionsRequest
                                       {
                                           BucketName = _bucketName,
                                           Prefix = id.AsS3ObjectKey(Tokens.ObjectPostfix),
                                           VersionIdMarker = nextVersionIdMarker
                                       });

                var nonDeletedVersions = response.Versions.FindAll(x => !x.IsDeleteMarker);
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

                var descriptors = new ObjectDescriptor[versionInfos.Count];
                var partitioner = Partitioner.Create(versionInfos);
                var tasks = partitioner.GetOrderablePartitions(_degreeOfParallelism)
                                       .Select(async partition =>
                                                   {
                                                       while (partition.MoveNext())
                                                       {
                                                           var index = partition.Current.Key;
                                                           var versionInfo = partition.Current.Value;

                                                           descriptors[index] = await GetObjectDescriptor(id, versionInfo.VersionId, CancellationToken.None);
                                                       }
                                                   });
                await Task.WhenAll(tasks);

                objectDescriptors.AddRange(descriptors);

                return (!initialVersionIdReached && response.IsTruncated, nextVersionIndex, response.NextVersionIdMarker);
            }

            var result = await ListVersions(0, null);
            if (objectDescriptors.Count == 0)
            {
                if (!await IsObjectExists(id))
                {
                    throw new ObjectNotFoundException($"Object '{id}' not found.");
                }

                return Array.Empty<ObjectVersionRecord>();
            }

            while (result.IsTruncated)
            {
                result = await ListVersions(result.NextVersionIndex, result.NextVersionIdMarker);
            }

            var maxVersionIndex = result.NextVersionIndex;
            var records = new ObjectVersionRecord[objectDescriptors.Count];
            for (var index = 0; index < objectDescriptors.Count; ++index)
            {
                var descriptor = objectDescriptors[index];
                records[index] = new ObjectVersionRecord(
                    descriptor.Id,
                    descriptor.VersionId,
                    --maxVersionIndex,
                    descriptor.LastModified,
                    new AuthorInfo(descriptor.Metadata.Author, descriptor.Metadata.AuthorLogin, descriptor.Metadata.AuthorName),
                    descriptor.Properties,
                    descriptor.Elements.Select(x => new ObjectVersionRecord.ElementRecord(x.TemplateCode, x.Value)).ToList(),
                    descriptor.Metadata.ModifiedElements);
            }

            return records;
        }

        public async Task<IReadOnlyCollection<VersionedObjectDescriptor<string>>> GetObjectLatestVersions(long id)
        {
            var versionsResponse = await _s3Client.ListVersionsAsync(_bucketName, id.ToString() + "/");
            return versionsResponse.Versions
                                   .Where(x => !x.IsDeleteMarker && x.IsLatest && !x.Key.EndsWith("/"))
                                   .Select(x => new VersionedObjectDescriptor<string>(x.Key, x.VersionId, x.LastModified))
                                   .ToList();
        }

        public async Task<ObjectDescriptor> GetObjectDescriptor(long id, string versionId, CancellationToken cancellationToken)
        {
            string objectVersionId;
            if (string.IsNullOrEmpty(versionId))
            {
                var objectVersions = await GetObjectLatestVersions(id);
                objectVersionId = objectVersions.Where(x => x.Id.EndsWith(Tokens.ObjectPostfix))
                                                .Select(x => x.VersionId)
                                                .SingleOrDefault();

                if (objectVersionId == null)
                {
                    throw new ObjectNotFoundException($"Object '{id}' not found.");
                }
            }
            else
            {
                objectVersionId = versionId;
            }

            var (persistenceDescriptor, objectAuthorInfo, objectLastModified, modifiedElements) =
                await GetObjectFromS3<ObjectPersistenceDescriptor>(id.AsS3ObjectKey(Tokens.ObjectPostfix), objectVersionId, cancellationToken);

            var tasks = persistenceDescriptor.Elements
                                             .Select(async x =>
                                                         {
                                                             var (elementPersistenceDescriptor, _, elementLastModified, _) =
                                                                 await GetObjectFromS3<ObjectElementPersistenceDescriptor>(x.Id, x.VersionId, cancellationToken);

                                                             SetCdnUris(id, objectVersionId, elementPersistenceDescriptor);
                                                             return new ObjectElementDescriptor
                                                                 {
                                                                     Id = x.Id.AsSubObjectId(),
                                                                     VersionId = x.VersionId,
                                                                     LastModified = elementLastModified,
                                                                     Type = elementPersistenceDescriptor.Type,
                                                                     TemplateCode = elementPersistenceDescriptor.TemplateCode,
                                                                     Properties = elementPersistenceDescriptor.Properties,
                                                                     Constraints = elementPersistenceDescriptor.Constraints,
                                                                     Value = elementPersistenceDescriptor.Value
                                                                 };
                                                         })
                                             .ToList();

            var elements = await Task.WhenAll(tasks);

            var descriptor = new ObjectDescriptor
                                 {
                                     Id = id,
                                     VersionId = objectVersionId,
                                     LastModified = objectLastModified,
                                     TemplateId = persistenceDescriptor.TemplateId,
                                     TemplateVersionId = persistenceDescriptor.TemplateVersionId,
                                     Language = persistenceDescriptor.Language,
                                     Properties = persistenceDescriptor.Properties,
                                     Elements = elements,
                                     Metadata = new ObjectDescriptor.ObjectMetadata
                                         {
                                             Author = objectAuthorInfo.Author,
                                             AuthorLogin = objectAuthorInfo.AuthorLogin,
                                             AuthorName = objectAuthorInfo.AuthorName,
                                             ModifiedElements = modifiedElements
                                         }
                                 };
            return descriptor;
        }

        public async Task<bool> IsObjectExists(long id)
        {
            var listResponse = await _s3Client.ListObjectsV2Async(
                                   new ListObjectsV2Request
                                   {
                                       BucketName = _bucketName,
                                       MaxKeys = 1,
                                       Prefix = $"{id}/{Tokens.ObjectPostfix}"
                                   });
            return listResponse.S3Objects.Count != 0;
        }

        public async Task<IImageElementValue> GetImageElementValue(long id, string versionId, int templateCode)
        {
            var objectDescriptor = await GetObjectDescriptor(id, versionId, CancellationToken.None);
            var element = objectDescriptor.Elements.SingleOrDefault(x => x.TemplateCode == templateCode);
            if (element == null)
            {
                throw new ObjectNotFoundException($"Element with template code '{templateCode}' of object/versionId '{id}/{versionId}' not found.");
            }

            if (!(element.Value is IImageElementValue elementValue))
            {
                throw new ArgumentException(
                    $"Element with template code '{templateCode}' of object/versionId '{id}/{versionId}' is not an image.",
                    nameof(templateCode));
            }

            return elementValue;
        }

        private async Task<(T, AuthorInfo, DateTime, IReadOnlyCollection<int>)> GetObjectFromS3<T>(string key, string versionId, CancellationToken cancellationToken)
        {
            try
            {
                using (var getObjectResponse = await _s3Client.GetObjectAsync(_bucketName, key, versionId, cancellationToken))
                {
                    var metadataWrapper = MetadataCollectionWrapper.For(getObjectResponse.Metadata);
                    var author = metadataWrapper.Read<string>(MetadataElement.Author);
                    var authorLogin = metadataWrapper.Read<string>(MetadataElement.AuthorLogin);
                    var authorName = metadataWrapper.Read<string>(MetadataElement.AuthorName);
                    var modifiedElements = metadataWrapper.Read<string>(MetadataElement.ModifiedElements);
                    var modifiedElementIds = string.IsNullOrEmpty(modifiedElements)
                                                 ? (IReadOnlyCollection<int>)Array.Empty<int>()
                                                 : modifiedElements.Split(Tokens.ModifiedElementsDelimiter).Select(int.Parse).ToList();
                    string content;
                    using (var reader = new StreamReader(getObjectResponse.ResponseStream, Encoding.UTF8))
                    {
                        content = reader.ReadToEnd();
                    }

                    var obj = JsonConvert.DeserializeObject<T>(content, SerializerSettings.Default);
                    return (obj, new AuthorInfo(author, authorLogin, authorName), getObjectResponse.LastModified, modifiedElementIds);
                }
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                throw new ObjectNotFoundException($"Object '{key}' with versionId '{versionId}' not found.");
            }
        }

        private void SetCdnUris(long id, string versionId, IObjectElementPersistenceDescriptor objectElementDescriptor)
        {
            if (!(objectElementDescriptor.Value is IBinaryElementValue binaryElementValue) || string.IsNullOrEmpty(binaryElementValue.Raw))
            {
                return;
            }

            binaryElementValue.DownloadUri = _cdnOptions.AsRawUri(binaryElementValue.Raw);

            switch (binaryElementValue)
            {
                case ICompositeBitmapImageElementValue compositeBitmapImageElementValue:
                    compositeBitmapImageElementValue.PreviewUri = _cdnOptions.AsPreviewUri(id, versionId, objectElementDescriptor.TemplateCode);
                    foreach (var image in compositeBitmapImageElementValue.SizeSpecificImages)
                    {
                        image.DownloadUri = _cdnOptions.AsRawUri(image.Raw);
                    }

                    break;
                case IImageElementValue imageElementValue:
                    imageElementValue.PreviewUri = _cdnOptions.AsRawUri(imageElementValue.Raw);
                    break;
            }
        }
    }
}
