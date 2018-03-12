using System;
using System.Collections.Generic;

using Newtonsoft.Json.Linq;

using NuClear.VStore.Descriptors;

namespace NuClear.VStore.DataContract
{
    public class TemplateVersionRecord : IIdentifyable<long>, IVersioned
    {
        private readonly AuthorInfo _authorInfo;
        private readonly VersionedObjectDescriptor<long> _versionedObjectDescriptor;

        public TemplateVersionRecord(
            long id,
            string versionId,
            int versionIndex,
            DateTime lastModified,
            AuthorInfo authorInfo,
            JObject properties,
            IReadOnlyCollection<int> elementTemplateCodes)
        {
            _authorInfo = authorInfo;
            _versionedObjectDescriptor = new VersionedObjectDescriptor<long>(id, versionId, lastModified);
            VersionIndex = versionIndex;
            Properties = properties;
            ElementTemplateCodes = elementTemplateCodes;
        }

        public long Id => _versionedObjectDescriptor.Id;
        public string VersionId => _versionedObjectDescriptor.VersionId;
        public int VersionIndex { get; }
        public string Author => _authorInfo.Author;
        public string AuthorLogin => _authorInfo.AuthorLogin;
        public string AuthorName => _authorInfo.AuthorName;
        public DateTime LastModified => _versionedObjectDescriptor.LastModified;
        public JObject Properties { get; }
        public IReadOnlyCollection<int> ElementTemplateCodes { get; set; }
    }
}