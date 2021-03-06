﻿using System;
using System.Collections.Generic;
using System.Linq;

using HtmlAgilityPack;

namespace NuClear.VStore.Objects.ContentValidation.FormattedUtils
{
    internal class UnsupportedListElementsSearcher : HtmlNodeVisitor
    {
        private bool _unsupportedListElementsFound;
        private readonly IReadOnlyCollection<string> _supportedTags;

        public UnsupportedListElementsSearcher(IReadOnlyCollection<string> supportedTags)
        {
            _supportedTags = supportedTags;
        }

        public bool IsThereUnsupportedElement(string text)
        {
            _unsupportedListElementsFound = false;
            var html = new HtmlDocument();
            html.LoadHtml(text);
            Visit(html.DocumentNode);
            return _unsupportedListElementsFound;
        }

        protected override void VisitElement(HtmlNode node)
        {
            if (node.Name.Equals(ElementFormattedTextTagNames.UnorderedList, StringComparison.OrdinalIgnoreCase))
            {
                _unsupportedListElementsFound |=
                    node.ChildNodes.Any(x => !_supportedTags.Contains(x.Name, StringComparer.OrdinalIgnoreCase));
            }

            if (_unsupportedListElementsFound)
            {
                return;
            }

            base.VisitElement(node);
        }

        protected override void Visit(HtmlNode node)
        {
            if (_unsupportedListElementsFound)
            {
                return;
            }

            base.Visit(node);
        }
    }
}
