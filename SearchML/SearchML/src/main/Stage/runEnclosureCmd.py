'''
Created on Jun 7, 2016

@author: gopasing
'''
import re
import subprocess
import io
import shlex

cdetEnclDict = {'cdetID': None, 'enclosure': None}

def getEnclosureList(cdetID):
    pat = re.compile(r"Diffs-(?P<typ>release|commit)*" +
                         r"-(comp_)*-*(?P<branch>\w*)" +
                         r"\.*(?P<part>\w*)")
    findcr_cmd = "/usr/cisco/bin/findcr -w Attachment-title -i %s" % cdetID
    args = shlex.split(findcr_cmd)
    print(findcr_cmd)
    listOfEnclosures = subprocess.getoutput(args)
    enclosures = listOfEnclosures.rstrip('\n').split(',')
    enclosures.sort()
    print(enclosures)

#     for encl in enclosures:
#         res = pat.match(encl)
#         if not res:
#             continue
# 
#         #typ = res.group('typ')
#         #branch = res.group('branch')
#         #if not typ:
#             # IOS component publication : Diffs--v153_3_s_xe310_throttle
#         #    typ = 'release'
# 
#         data = {'cdetID': cdetID, 'enclosure': encl}
#         #if not enclosures[typ].get(branch):
#         #    enclosures[typ][branch] = []
#         #self._parse_enclosure(data)
#         #self.enclosures[typ][branch].append(data)
#         return data

def main():
    with open("/scratch/cdets.txt/part-00000","r") as f:
        content = [x.rstrip('\n') for x in f.readlines()]
        print(content)
        for cdet in content:
            getEnclosureList(cdet)


if __name__=="__main__":
    print("start")
    main()