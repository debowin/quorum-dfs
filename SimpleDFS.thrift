service NodeService {
	// exposed services
	bool write(1: string filename, 2: string content),
    string read(1: string filename),
	// quorum services
	bool quorumWrite(1: string filename, 2: string content, 3: i32 version),
	string quorumRead(1: string filename),
	i32 quorumVersion(1: string filename),
	// coordinator services
	bool coordWrite(1: string filename, 2: string content),
    string coordRead(1: string filename),
}