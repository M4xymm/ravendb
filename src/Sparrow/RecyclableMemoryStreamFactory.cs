﻿using System;
using System.IO;
using Microsoft.IO;
using Sparrow.Global;

namespace Sparrow;

internal class RecyclableMemoryStreamFactory
{
    private static readonly RecyclableMemoryStreamManager Manager = new()
    {
        Settings =
        {
            AggressiveBufferReturn = true,
            MaximumBufferSize = Constants.Size.Megabyte,
            MaximumSmallPoolFreeBytes = 256 * Constants.Size.Megabyte,
            MaximumLargePoolFreeBytes = 128 * Constants.Size.Megabyte,
            ThrowExceptionOnToArray = true
        }
    };

    public static RecyclableMemoryStream GetRecyclableStream()
    {
        return Manager.GetStream(Guid.Empty);
    }

    public static RecyclableMemoryStream GetRecyclableStream(long requiredSize)
    {
        return Manager.GetStream(Guid.Empty, null, requiredSize);
    }

    public static MemoryStream GetMemoryStream()
    {
        return new MemoryStream();
    }
}
