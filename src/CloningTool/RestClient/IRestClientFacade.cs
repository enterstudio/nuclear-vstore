﻿using System;
using System.Net.Http.Headers;
using System.Threading.Tasks;

using CloningTool.Json;

using NuClear.VStore.Descriptors.Templates;

namespace CloningTool.RestClient
{
    public interface IRestClientFacade : IReadOnlyRestClientFacade
    {
        Task CreatePositionTemplateLinkAsync(string positionId, string templateId);
        Task<string> CreateTemplateAsync(string templateId, TemplateDescriptor template);
        Task<string> UpdateTemplateAsync(TemplateDescriptor template, string versionId);
        Task<ApiObjectDescriptor> CreateAdvertisementPrototypeAsync(long templateId, string langCode, long firmId);
        Task<ApiObjectElementRawValue> UploadFileAsync(long advertisementId, Uri uploadUri, string fileName, byte[] fileData, params NameValueHeaderValue[] headers);
        Task UpdateAdvertisementModerationStatusAsync(string objectId, string versionId, ModerationResult moderationResult);
        Task SelectAdvertisementToWhitelistAsync(string advertisementId);
        Task<string> CreateAdvertisementAsync(long id, long firmId, ApiObjectDescriptor advertisement);
        Task<ApiObjectDescriptor> UpdateAdvertisementAsync(ApiObjectDescriptor advertisement);
    }
}
