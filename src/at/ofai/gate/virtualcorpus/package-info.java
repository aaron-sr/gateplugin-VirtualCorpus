/**
 * This package implements the DirectoryCorpus plugin which
 * provides a new corpus LR, the DirectoryCorpus LR.
 *
 * A DirectoryCorpus is "backed" by the files in a directory. The URL of that
 * directory has to be provided when creating the DirectoryCorpus LR.
 * In addition, a different directory can be specified where modified documents
 * are written to (instead of the original directory) and the corpus can be
 * made read-only so no changed documents are ever written back to the file
 * system.
 *
 * The DirectoryCorpus Language Resource allows the following parameters
 * at creation time:
 * <ul>
 * <li><b>directoryURL</b>: The URL of the directory where the files
 * for the corpus will
 * be read from. If the <code>outDirectoryURL</code> is left empty the documents
 * will be written back to the original files in this directory when
 * unloaded (except when <code>saveDocuments</code> is set to false).</li>
 * <li><b>outDirectoryURL</b>: The URL of a directory where modfied documents
 * are stored.
 * If this is empty then the directoryURL will be used for both reading
 * and storing files. If this is provided, the files from the directoryURL
 * will not be overwritten and can have file extensions other than ".xml"
 * of no file extension at all. The files writting in this directory will
 * always have their file extension set to ".xml" either by replacing an
 * existing extension or appending it. <br/>
 * NOTE: Any existing files in this directory can be overwritten. Also, if
 * the directoryURL contains several files which only differ in their
 * file extension, they will all be written to the same file with extension
 * ".xml".</li>
 * <li><b>saveDocuments</b>: If set to false, nothing will be writte to the file
 * system. If set to true (the default), then modified documents are written
 * to either the directoryURL or, if specified, the outDirectoryURL directory.
 * </li>
 * </ul>
 * All files in this directory that conform to the following constraints are
 * included in the corpus:
 * <ul>
 * <li>The filename does not start with a dot (Linux hidden files are ignored)</li>
 * <li>The file extension is ".xml" (in any combination of lower/upper case).
 * This is only necessary when no outDirectoryURL parameter is specified.</li>
 * <li>The filename is not longer than 200 characters</li>
 * <li>The filename does not contain any of the following characters: slash(/),
 * question mark(?), double quote("), less-than(<), greater-than(>), bar(|),
 * or colon(:). 
 * </ul>
 * All other files are ignored.
 * <p/>
 * None of the files are actually read until accessed from the corpus. When
 * a document is unloaded (closed in the GUI or finished processing in a
 * controller) it is written back as a GATE XML document, overwriting the
 * original file. If the parameter <code>outDirectoryURL</code> is specified,
 * the document is saved to a file in that directory instead and the original
 * file remains unchanged. If the parameter </code>saveDocuments</code> is
 * set to <code>false</code> nothing is written to the file system.
 * <p/>
 * The original files in the <code>directoryURL</code> directory can be either
 * GATE format XML files or arbitrary XML files and will be read in as
 * expected. When these files are saved, they are always saved in GATE XML
 * format though.
 * <p/>
 * This corpus implementation does not support the "populate" methods or
 * GUI options and using the GUI option "Save as XML" does not make sense
 * with this corpus either.
 */
package at.ofai.gate.virtualcorpus;
