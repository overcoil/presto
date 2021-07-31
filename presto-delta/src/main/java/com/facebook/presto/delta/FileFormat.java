/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// probably should be contributed to DSR
package io.delta.standalone; // cribbed from org.apache.iceberg; license text adjusted & enum values trimmed

/**
 * Enum of supported file formats.
 */
public enum FileFormat {
    PARQUET("parquet", true);

    private final String ext;
    private final boolean splittable;

    FileFormat(String ext, boolean splittable)
    {
        this.ext = "." + ext;
        this.splittable = splittable;
    }

    public boolean isSplittable()
    {
        return splittable;
    }

    /**
     * Returns filename with this format's extension added, if necessary.
     *
     * @param filename a filename or path
     * @return if the ext is present, the filename, otherwise the filename with ext added
     */
    public String addExtension(String filename)
    {
        if (filename.endsWith(ext)) {
            return filename;
        }
        return filename + ext;
    }

    public static FileFormat fromFileName(CharSequence filename)
    {
        // TODO: really need to do a check here
        return PARQUET;
    }
}
