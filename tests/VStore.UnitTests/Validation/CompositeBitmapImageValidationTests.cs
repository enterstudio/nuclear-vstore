﻿using System;
using System.IO;

using NuClear.VStore.Descriptors;
using NuClear.VStore.Descriptors.Templates;
using NuClear.VStore.Sessions;
using NuClear.VStore.Sessions.ContentValidation;
using NuClear.VStore.Sessions.ContentValidation.Errors;

using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats;
using SixLabors.ImageSharp.Formats.Bmp;
using SixLabors.ImageSharp.Formats.Gif;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Formats.Png;

using Xunit;

namespace VStore.UnitTests.Validation
{
    public class CompositeBitmapImageValidationTests
    {
        [Theory]
        [InlineData(FileFormat.Png)]
        [InlineData(FileFormat.Gif)]
        [InlineData(FileFormat.Jpeg)]
        [InlineData(FileFormat.Jpg)]
        public void ValidOriginalFormat(FileFormat format)
        {
            var encoder = GetEncoder(format);
            var image = CreateImage(5, 5, encoder);
            var constraints = CreateConstraints(1, 1, 10, 10, format);

            BitmapImageValidator.ValidateCompositeBitmapImageOriginalHeader(1, constraints, format, image);
        }

        [Theory]
        [InlineData(FileFormat.Png, FileFormat.Jpeg)]
        [InlineData(FileFormat.Png, FileFormat.Jpg)]
        [InlineData(FileFormat.Png, FileFormat.Gif)]
        [InlineData(FileFormat.Gif, FileFormat.Png)]
        [InlineData(FileFormat.Gif, FileFormat.Jpeg)]
        [InlineData(FileFormat.Gif, FileFormat.Jpg)]
        [InlineData(FileFormat.Jpg, FileFormat.Gif)]
        [InlineData(FileFormat.Jpg, FileFormat.Png)]
        [InlineData(FileFormat.Jpeg, FileFormat.Gif)]
        [InlineData(FileFormat.Jpeg, FileFormat.Png)]
        public void InvalidOriginalFormat(FileFormat expectedFormat, FileFormat actualFormat)
        {
            var encoder = GetEncoder(actualFormat);
            var image = CreateImage(5, 5, encoder);
            var constraints = CreateConstraints(1, 1, 10, 10, expectedFormat);

            var ex = Assert.Throws<InvalidBinaryException>(() => BitmapImageValidator.ValidateCompositeBitmapImageOriginalHeader(1, constraints, actualFormat, image));
            Assert.IsType<BinaryInvalidFormatError>(ex.Error);
        }

        [Theory]
        [InlineData(5, 5, 1, 1, 10, 10)]
        [InlineData(5, 5, 1, 1, 5, 5)]
        [InlineData(5, 5, 5, 5, 10, 10)]
        [InlineData(5, 5, 5, 5, 5, 5)]
        public void ValidOriginalSize(int width, int height, int minWidth, int minHeight, int maxWidth, int maxHeight)
        {
            var image = CreateImage(width, height, new PngEncoder());
            const FileFormat PngFormat = FileFormat.Png;
            var constraints = CreateConstraints(minWidth, minHeight, maxWidth, maxHeight, PngFormat);

            BitmapImageValidator.ValidateCompositeBitmapImageOriginalHeader(1, constraints, PngFormat, image);
        }

        [Theory]
        [InlineData(1, 1, 5, 5, 10, 10)]
        [InlineData(1, 7, 5, 5, 10, 10)]
        [InlineData(7, 1, 5, 5, 10, 10)]
        [InlineData(7, 11, 5, 5, 10, 10)]
        public void InvalidOriginalSize(int width, int height, int minWidth, int minHeight, int maxWidth, int maxHeight)
        {
            var image = CreateImage(width, height, new GifEncoder());
            const FileFormat GifFormat = FileFormat.Gif;
            var constraints = CreateConstraints(minWidth, minHeight, maxWidth, maxHeight, GifFormat);

            var ex = Assert.Throws<InvalidBinaryException>(() => BitmapImageValidator.ValidateCompositeBitmapImageOriginalHeader(1, constraints, GifFormat, image));
            Assert.IsType<ImageSizeOutOfRangeError>(ex.Error);
        }

        [Fact]
        public void ValidSizeSpecificImage()
        {
            var width = 5;
            var height = 5;

            var image = CreateImage(width, height, new GifEncoder());
            const FileFormat GifFormat = FileFormat.Gif;
            var constraints = CreateConstraints(1, 1, 10, 10, GifFormat);

            BitmapImageValidator.ValidateSizeSpecificBitmapImageHeader(1, constraints, GifFormat, image, new ImageSize {Width = width, Height = height});
        }

        [Fact]
        public void NotSquaredSizeSpecificImage()
        {
            var width = 5;
            var height = 4;

            var image = CreateImage(width, height, new JpegEncoder());
            const FileFormat JpegFormat = FileFormat.Jpeg;
            var constraints = CreateConstraints(1, 1, 10, 10, JpegFormat);


            var ex = Assert.Throws<InvalidBinaryException>(
                () => BitmapImageValidator.ValidateSizeSpecificBitmapImageHeader(1, constraints, JpegFormat, image, new ImageSize {Width = width, Height = height}));
            Assert.IsType<SizeSpecificImageIsNotSquareError>(ex.Error);
        }

        [Fact]
        public void SizeSpecificImageSizeNotEqualToTargetSize()
        {
            var image = CreateImage(5, 5, new PngEncoder());
            const FileFormat PngFormat = FileFormat.Png;
            var constraints = CreateConstraints(1, 1, 10, 10, PngFormat);


            var ex = Assert.Throws<InvalidBinaryException>(
                () => BitmapImageValidator.ValidateSizeSpecificBitmapImageHeader(1, constraints, PngFormat, image, new ImageSize {Width = 4, Height = 4}));
            Assert.IsType<SizeSpecificImageTargetSizeNotEqualToActualSizeError>(ex.Error);
        }

        private static Stream CreateImage(int width, int height, IImageEncoder encoder)
        {
            var image = Image.LoadPixelData(new Rgba32[width * height], width, height);
            var ms = new MemoryStream();
            image.Save(ms, encoder);
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        private static CompositeBitmapImageElementConstraints CreateConstraints(int minWidth, int minHeight, int maxWidth, int maxHeight, params FileFormat[] formats) =>
            new CompositeBitmapImageElementConstraints
                {
                    ImageSizeRange = new ImageSizeRange
                        {
                            Min = new ImageSize { Height = minHeight, Width = minWidth },
                            Max = new ImageSize { Height = maxHeight, Width = maxWidth }
                        },
                    SupportedFileFormats = formats
                };

        private static IImageEncoder GetEncoder(FileFormat format)
        {
            switch (format)
            {
                case FileFormat.Png:
                    return new PngEncoder();
                case FileFormat.Gif:
                    return new GifEncoder();
                case FileFormat.Bmp:
                    return new BmpEncoder();
                case FileFormat.Jpg:
                case FileFormat.Jpeg:
                    return new JpegEncoder();
                default:
                    throw new ArgumentOutOfRangeException(nameof(format), format, null);
            }
        }
    }
}