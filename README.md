streamsx.document
=================

This toolkit allows extract text and metadata from documents in a binary formats
such as PDF, Word, Office, etc. For this purpose the toolkit implements a DocumentSource operator.

The DocumentSource operator utilized multiple third party and open source document extraction technologies, 
and can be enhanced with additional commercial /proprietary extractors. The operator automatically determines 
the document MIME type and delegated the extraction request to appropriate extractor plugin.

Out of the box the toolkit provides the following extractors:
 *  Apache Tika – The primary extractor for binary documents such as Office documents (Word, Powerpoint, Excel), HTML files, etc.
 *  PDFBox – For handling Acrobat PDF files
 *  TrueZIP – ZIP, JAR, TAR, GZ, GZIP files and other archive files
 *  JUnrar – RAR files
 *  Plain Text – Text files of various encodings (ASCII, UTF-8, UTF-16, local encodings)

The toolkit's home page is available at:
http://ibmstreams.github.io/streamsx.document/

