﻿using System;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging;

using NuClear.VStore.Descriptors;
using NuClear.VStore.Host.Filters;
using NuClear.VStore.Sessions;

namespace NuClear.VStore.Host.Controllers
{
    [Route("session")]
    public sealed class SessionController : Controller
    {
        private readonly SessionManagementService _sessionManagementService;
        private readonly ILogger<SessionController> _logger;

        public SessionController(SessionManagementService sessionManagementService, ILogger<SessionController> logger)
        {
            _sessionManagementService = sessionManagementService;
            _logger = logger;
        }

        [HttpPost("{templateId}/{language}")]
        public async Task<IActionResult> SetupSession(long templateId, Language language)
        {
            try
            {
                var sessionSetupContext = await _sessionManagementService.Setup(templateId, language);
                var templateDescriptor = sessionSetupContext.TemplateDescriptor;

                var uploadUrls = UploadUrl.Generate(
                    templateDescriptor,
                    templateCode => Url.Action(
                        "UploadFile",
                        new
                            {
                                sessionId = sessionSetupContext.Id,
                                templateCode
                            }));

                return Json(
                    new
                        {
                            Template = new
                                           {
                                               Id = templateId,
                                               templateDescriptor.VersionId,
                                               templateDescriptor.Properties,
                                               templateDescriptor.Elements
                                           },
                            uploadUrls,
                            sessionSetupContext.ExpiresAt
                        });
            }
            catch (SessionCannotBeCreatedException ex)
            {
                return StatusCode(422, ex.Message);
            }
        }

        [HttpPost("{sessionId}/upload/{templateCode}")]
        [DisableFormValueModelBinding]
        [MultipartBodyLengthLimit(1024)]
        public async Task<IActionResult> UploadFile(Guid sessionId, int templateCode)
        {
            var multipartBoundary = Request.GetMultipartBoundary();
            if (string.IsNullOrEmpty(multipartBoundary))
            {
                return BadRequest($"Expected a multipart request, but got '{Request.ContentType}'.");
            }

            MultipartUploadSession uploadSession = null;
            var reader = new MultipartReader(multipartBoundary, HttpContext.Request.Body);
            var section = await reader.ReadNextSectionAsync();
            var contentLength = HttpContext.Request.ContentLength;
            if (section == null || contentLength == null)
            {
                return BadRequest("Request body is empty or doesn't contain sections.");
            }

            try
            {
                for (; section != null; section = await reader.ReadNextSectionAsync())
                {
                    var fileSection = section.AsFileSection();
                    if (fileSection == null)
                    {
                        if (uploadSession != null)
                        {
                            await _sessionManagementService.AbortMultipartUpload(uploadSession);
                        }

                        return BadRequest("File upload supported only during single request.");
                    }

                    if (uploadSession == null)
                    {
                        uploadSession = await _sessionManagementService.InitiateMultipartUpload(
                                            sessionId,
                                            fileSection.FileName,
                                            section.ContentType,
                                            contentLength.Value,
                                            templateCode);
                        _logger.LogInformation($"Multipart upload for file '{fileSection.FileName}' was initiated.");
                    }

                    using (fileSection.FileStream)
                    {
                        await _sessionManagementService.UploadFilePart(uploadSession, fileSection.FileStream, templateCode);
                    }
                }

                var uploadedFileInfo = await _sessionManagementService.CompleteMultipartUpload(uploadSession, templateCode);
                return Json(
                    new
                        {
                            uploadedFileInfo.Id,
                            uploadedFileInfo.Filename,
                            uploadedFileInfo.PreviewUri
                        });
            }
            catch (Exception ex)
            {
                _logger.LogError(new EventId(0), ex, "Error occured while file uploading.");
                if (uploadSession != null)
                {
                    await _sessionManagementService.AbortMultipartUpload(uploadSession);
                }

                return StatusCode(422, ex.Message);
            }
        }
    }
}
