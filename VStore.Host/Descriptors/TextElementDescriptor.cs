﻿namespace NuClear.VStore.Host.Descriptors
{
    public sealed class TextElementDescriptor : ITextElementDescriptor
    {
        public ElementDescriptorType Type => ElementDescriptorType.Text;
        public bool IsMandatory { get; set; }
        public int? MinSymbolsPerWord { get; set; }
        public int? MaxSymbolsPerWord { get; set; }
        public int? MaxLines { get; set; }
        public bool IsFormatted { get; set; }
    }
}