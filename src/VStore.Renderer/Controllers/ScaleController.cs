﻿using System;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Mvc;

using Newtonsoft.Json.Linq;

using NuClear.VStore.Http.Core.Controllers;
using NuClear.VStore.ImageRendering;
using NuClear.VStore.Json;
using NuClear.VStore.Objects;
using NuClear.VStore.Options;
using NuClear.VStore.S3;
using NuClear.VStore.Sessions;

namespace NuClear.VStore.Renderer.Controllers
{
    [ApiVersion("1.0")]
    [Route("scale")]
    public class ScaleController : VStoreController
    {
        private readonly TimeSpan _retryAfter;
        private readonly ObjectsStorageReader _objectsStorageReader;
        private readonly ImagePreviewService _imagePreviewService;

        public ScaleController(
            ThrottlingOptions throttlingOptions,
            ObjectsStorageReader objectsStorageReader,
            ImagePreviewService imagePreviewService)
        {
            _retryAfter = throttlingOptions.RetryAfter;
            _objectsStorageReader = objectsStorageReader;
            _imagePreviewService = imagePreviewService;
        }

        [HttpGet("{id:long}/{versionId}/{templateCode:int}/x{height:int}")]
        [ProducesResponseType(typeof(byte[]), 200)]
        [ProducesResponseType(typeof(string), 400)]
        [ProducesResponseType(404)]
        [ProducesResponseType(typeof(object), 422)]
        [ProducesResponseType(429)]
        public async Task<IActionResult> GetByHeight(long id, string versionId, int templateCode, int height)
        {
            if (height < 1)
            {
                return BadRequest("Incorrect height");
            }

            return await GetInternal(id, versionId, templateCode, height);
        }

        [HttpGet("{id:long}/{versionId}/{templateCode:int}/{width:int}x")]
        [ProducesResponseType(typeof(byte[]), 200)]
        [ProducesResponseType(typeof(string), 400)]
        [ProducesResponseType(404)]
        [ProducesResponseType(typeof(object), 422)]
        [ProducesResponseType(429)]
        public async Task<IActionResult> GetByWidth(long id, string versionId, int templateCode, int width)
        {
            if (width < 1)
            {
                return BadRequest("Incorrect width");
            }

            return await GetInternal(id, versionId, templateCode, width);
        }

        [HttpGet("{id:long}/{versionId}/{templateCode:int}/{width:int}x{height:int}")]
        [ProducesResponseType(typeof(byte[]), 200)]
        [ProducesResponseType(typeof(string), 400)]
        [ProducesResponseType(404)]
        [ProducesResponseType(typeof(object), 422)]
        [ProducesResponseType(429)]
        public async Task<IActionResult> Get(long id, string versionId, int templateCode, int width, int height)
        {
            if (width < 1 || height < 1)
            {
                return BadRequest("Incorrect width or height");
            }

            return await GetInternal(id, versionId, templateCode, Math.Max(height, width));
        }

        private async Task<IActionResult> GetInternal(long id, string versionId, int templateCode, int sideLength)
        {
            try
            {
                var imageElementValue = await _objectsStorageReader.GetImageElementValue(id, versionId, templateCode);
                var (imageStream, contentType) = await _imagePreviewService.GetScaledPreview(imageElementValue, templateCode, sideLength);
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
            catch (ArgumentException ex)
            {
                return BadRequest(ex.Message);
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