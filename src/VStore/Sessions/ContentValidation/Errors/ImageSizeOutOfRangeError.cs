﻿using Newtonsoft.Json.Linq;

using NuClear.VStore.Descriptors;
using NuClear.VStore.Descriptors.Templates;
using NuClear.VStore.Json;

namespace NuClear.VStore.Sessions.ContentValidation.Errors
{
    public sealed class ImageSizeOutOfRangeError : BinaryValidationError
    {
        public ImageSizeOutOfRangeError(ImageSize imageSize)
        {
            ImageSize = imageSize;
        }

        public ImageSize ImageSize { get; }

        public override string ErrorType => nameof(CompositeBitmapImageElementConstraints.ImageSizeRange);

        public override JToken SerializeToJson()
        {
            var ret = base.SerializeToJson();
            ret[Tokens.ValueToken] = JToken.FromObject(ImageSize, JsonSerializer);
            return ret;
        }
    }
}