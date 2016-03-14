def countnodes(infile):
    distinct = set()
    for line in infile:
        paperIDs = line.split()
        distinct.add(paperIDs[0])
        distinct.add(paperIDs[1])
    infile.seek(0)
    return len(distinct)


def makedir(dir_name):
    import os
    output_dir = '.'  # TODO make this a config option
    output_subdir = '%s/%s' % (output_dir, dir_name)
    if not os.path.isdir(output_subdir):
        os.makedirs(output_subdir)
