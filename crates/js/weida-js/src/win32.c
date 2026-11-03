/*
 * Loading on Windows. The addon links node.lib (imports from node.exe) and
 * weida.dll.lib, both delay-loaded (/DELAYLOAD). This hook binds node.exe to
 * the executable that loaded the addon, because node.exe, bun.exe and
 * deno.exe each export Node-API; without it, Bun calls into a foreign
 * node.exe and crashes (oven-sh/bun#13566). It also binds weida.dll to the
 * copy beside weida.node, whatever DLL search order the runtime's loader
 * uses. Only build.ps1 compiles this file.
 */
#include "addon.h"

#include <delayimp.h>
#include <string.h>
#include <wchar.h>

static HMODULE libweida;
static INIT_ONCE once = INIT_ONCE_STATIC_INIT;

static BOOL CALLBACK load_once(PINIT_ONCE o, PVOID p, PVOID *c) {
    static WCHAR path[32768];
    HMODULE self;
    (void)o;
    (void)p;
    (void)c;
    if (!GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                                GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                            (LPCWSTR)(void *)&libweida, &self))
        return TRUE;
    DWORD n = GetModuleFileNameW(self, path, 32768);
    if (n == 0 || n >= 32768)
        return TRUE;
    WCHAR *sep = wcsrchr(path, L'\\');
    if (!sep)
        return TRUE;
    sep++;
    if (wcscpy_s(sep, 32768 - (size_t)(sep - path), L"weida.dll") != 0)
        return TRUE;
    libweida = LoadLibraryExW(path, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
    return TRUE;
}

bool load_libweida(void) {
    InitOnceExecuteOnce(&once, load_once, NULL, NULL);
    return libweida != NULL;
}

static FARPROC WINAPI delay_hook(unsigned notify, PDelayLoadInfo info) {
    if (notify != dliNotePreLoadLibrary)
        return NULL;
    if (_stricmp(info->szDll, "node.exe") == 0)
        return (FARPROC)GetModuleHandleW(NULL);
    if (_stricmp(info->szDll, "weida.dll") == 0 && load_libweida())
        return (FARPROC)libweida;
    return NULL;
}

const PfnDliHook __pfnDliNotifyHook2 = delay_hook;
