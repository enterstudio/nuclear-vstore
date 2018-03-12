﻿using System;

namespace NuClear.VStore.Descriptors
{
    public sealed class VersionedObjectDescriptor<TId> : IIdentifyable<TId>, IVersioned, IEquatable<VersionedObjectDescriptor<TId>>
        where TId : IEquatable<TId>
    {
        public VersionedObjectDescriptor(TId id, string versionId, DateTime lastModified)
        {
            Id = id;
            VersionId = versionId;
            LastModified = lastModified;
        }

        public TId Id { get; }

        public string VersionId { get; }

        public DateTime LastModified { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is VersionedObjectDescriptor<TId> other))
            {
                return false;
            }

            return ReferenceEquals(this, other) || Equals(other);
        }

        public bool Equals(VersionedObjectDescriptor<TId> other) =>
            Id.Equals(other.Id) && (VersionId?.Equals(other.VersionId, StringComparison.OrdinalIgnoreCase) ?? true);

        public override int GetHashCode()
        {
            unchecked
            {
                return (Id.GetHashCode() * 397) ^ (VersionId?.GetHashCode() ?? 0);
            }
        }
    }
}