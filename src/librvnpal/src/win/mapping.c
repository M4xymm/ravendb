#include <windows.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

EXPORT int32_t
rvn_discard_virtual_memory(void* address, int64_t size, int32_t* detailed_error_code)
{
    if (IsWindows10OrGreater() == false)
        return 0;
    return DiscardVirtualMemory(address, size);
}
