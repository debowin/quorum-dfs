exception DFSError {
	1: string msg
}

service NodeService {
	// exposed services
	bool write(1: string filename, 2: string content)  throws (1: DFSError dfse),
    string read(1: string filename) throws (1: DFSError dfse),
    map<string, i32> ls() throws (1: DFSError dfse),
	// quorum services
	bool quorumWrite(1: string filename, 2: string content, 3: i32 version) throws (1: DFSError dfse),
	string quorumRead(1: string filename) throws (1: DFSError dfse),
	i32 quorumVersion(1: string filename) throws (1: DFSError dfse),
	// coordinator services
	bool coordWrite(1: string filename, 2: string content) throws (1: DFSError dfse),
    string coordRead(1: string filename) throws (1: DFSError dfse),
    map<string, i32> coordLS() throws (1: DFSError dfse)
}