﻿namespace NuClear.VStore.Objects.ContentValidation.Errors
{
    public class BinaryInvalidFormatError : ObjectElementValidationError
    {
        public override ElementConstraintViolations ErrorType => ElementConstraintViolations.SupportedFileFormats;
    }
}