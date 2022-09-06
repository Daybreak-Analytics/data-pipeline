import os
filePath = './rawdata/'
savePath = './EventData/'

if not os.path.exists(savePath):
    os.makedirs(savePath)

print("Start Replace Raw Data File")
for i,j,k in os.walk(filePath):
    for file in k:
        if file.endswith(".csv"): 
            with open(i+'/'+file) as f:
                lines = f.readlines()
            with open(savePath + i.replace('./rawdata/EventId=','') +'.csv','w+') as write_file:
                for l in lines:
                    write_file.write(l.replace("\"", "").replace("*", ""))
print("End Replace Raw Data File")