﻿using System;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Mvc;

using Newtonsoft.Json.Linq;

using NuClear.VStore.Descriptors.Objects;
using NuClear.VStore.Http.Core.Controllers;
using NuClear.VStore.ImageRendering;
using NuClear.VStore.Json;
using NuClear.VStore.Objects;
using NuClear.VStore.Options;
using NuClear.VStore.S3;
using NuClear.VStore.Sessions;

namespace NuClear.VStore.Renderer.Controllers
{
    [ApiVersion("2.0")]
    [ApiVersion("1.0", Deprecated = true)]
    [Route("previews")]
    public sealed class PreviewController : VStoreController
    {
        private readonly TimeSpan _retryAfter;
        private readonly RawFileStorageInfoProvider _rawFileStorageInfoProvider;
        private readonly ObjectsStorageReader _objectsStorageReader;
        private readonly ImagePreviewService _imagePreviewService;

        public PreviewController(
            ThrottlingOptions throttlingOptions,
            RawFileStorageInfoProvider rawFileStorageInfoProvider,
            ObjectsStorageReader objectsStorageReader,
            ImagePreviewService imagePreviewService)
        {
            _retryAfter = throttlingOptions.RetryAfter;
            _rawFileStorageInfoProvider = rawFileStorageInfoProvider;
            _objectsStorageReader = objectsStorageReader;
            _imagePreviewService = imagePreviewService;
        }

        [MapToApiVersion("2.0")]
        [HttpGet("{id:long}/{versionId}/{templateCode:int}/image_{width:int}x{height:int}.png")]
        [ProducesResponseType(typeof(byte[]), 200)]
        [ProducesResponseType(302)]
        [ProducesResponseType(400)]
        [ProducesResponseType(404)]
        [ProducesResponseType(typeof(object), 422)]
        [ProducesResponseType(429)]
        public async Task<IActionResult> Get(long id, string versionId, int templateCode, int width, int height)
        {
            try
            {
                var imageElementValue = await _objectsStorageReader.GetImageElementValue(id, versionId, templateCode);
                if (imageElementValue.TryGetSizeSpecificBitmapImageRawValue(width, height, out var rawValue))
                {
                    return Redirect(_rawFileStorageInfoProvider.GetRawFileUrl(rawValue));
                }

                var (imageStream, contentType) = await _imagePreviewService.GetPreview(imageElementValue, templateCode, width, height);
                return new FileStreamResult(imageStream, contentType);
            }
            catch (ObjectNotFoundException)
            {
                return NotFound();
            }
            catch (OperationCanceledException)
            {
                return TooManyRequests(_retryAfter);
            }
            catch (MemoryLimitedException)
            {
                return TooManyRequests(_retryAfter);
            }
            catch (ArgumentException)
            {
                return BadRequest();
            }
            catch (InvalidBinaryException ex)
            {
                return Unprocessable(GenerateErrorJsonResult(ex));
            }
        }

        [Obsolete, MapToApiVersion("1.0")]
        [HttpGet("{id:long}/{versionId}/{templateCode:int}/image_{width:int}x{height:int}.png")]
        [ProducesResponseType(typeof(byte[]), 200)]
        [ProducesResponseType(302)]
        [ProducesResponseType(400)]
        [ProducesResponseType(404)]
        [ProducesResponseType(typeof(object), 422)]
        [ProducesResponseType(429)]
        public async Task<IActionResult> GetV10(long id, string versionId, int templateCode, int width, int height)
        {
            try
            {
                var imageElementValue = await _objectsStorageReader.GetImageElementValue(id, versionId, templateCode);
                if (imageElementValue.TryGetSizeSpecificBitmapImageRawValue(width, height, out var rawValue))
                {
                    return Redirect(_rawFileStorageInfoProvider.GetRawFileUrl(rawValue));
                }

                var (imageStream, contentType) = await _imagePreviewService.GetRoundedPreview(imageElementValue, templateCode, width, height);
                return new FileStreamResult(imageStream, contentType);
            }
            catch (ObjectNotFoundException)
            {
                return NotFound();
            }
            catch (OperationCanceledException)
            {
                return TooManyRequests(_retryAfter);
            }
            catch (MemoryLimitedException)
            {
                return TooManyRequests(_retryAfter);
            }
            catch (ArgumentException)
            {
                return BadRequest();
            }
            catch (InvalidBinaryException ex)
            {
                return Unprocessable(GenerateErrorJsonResult(ex));
            }
        }

        private static JToken GenerateErrorJsonResult(InvalidBinaryException ex) =>
            new JObject
                {
                    { Tokens.ErrorsToken, new JArray() },
                    { Tokens.ElementsToken, new JArray { ex.SerializeToJson() } }
                };
    }
}