package com.distributed.filesystem;

import java.io.Serializable;

/**
 * Represents a file operation in the distributed file system
 */
public enum FileOperation implements Serializable {
    UPLOAD,
    DOWNLOAD,
    DELETE,
    SEARCH
}

