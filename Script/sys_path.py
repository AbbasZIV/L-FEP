def AddSysPath(new_path):
    import sys, os
    #Avoid adding nonexistent paths
    if not os.path.exists(new_path): return -1

    #Check against all currently available paths
    for x in sys.path:
        x = os.path.abspath(x)
        if new_path in (x, x + os.sep):
            return 0
    sys.path.append(new_path)
    return 1