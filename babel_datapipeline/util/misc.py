def countnodes(infile):
    distinct = set()
    for line in infile:
        paperIDs = line.split(' ')
        distinct.add(paperIDs[0])
        distinct.add(paperIDs[1])
    infile.seek(0)
    return len(distinct)