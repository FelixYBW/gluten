jsonfile='noparallel_nosplit_rowgroup1_dib_pct0_distance256m_256mloadquantum_64mdistance_2iothread'
with open("/home/binweiyang/spark/work/app-20231209120324-0069/2/stdout") as f:
    out=f.readlines()
out2=[l.replace(": "," ").replace("]","] ").replace("\t"," ").replace("\n","") for l in out]
out3=[]
#out4=[]
starttime=0
dur=0
st=0
last_end=0
last_start=-1
for l in out2:
    t=re.search("LATENCY_BREAKDOWN \[(.+)\] (\d+) (\d+) (\d+) (\d+)",l)
    if t:
        if starttime==0:
            starttime = int(t.group(3))
        if int(t.group(4))>dur:
            dur=int(t.group(4))
        if int(t.group(3))-starttime>500670677 or int(t.group(4))>20000000:
            continue
        slipstart=int(t.group(3))-starttime+1
        dur=int(t.group(4))
        
        out3.append({
            'tid':int(t.group(2)[-5:]),
            'ts':slipstart,
            'dur':dur,
            'pid':0,
            'ph':'X',
            'name':t.group(1),
            "args":{"size":human_format(int(t.group(5)))}
        })
        last_end=slipstart+dur
        last_start=slipstart
        #out4.append([int(t.group(2)[-5:]),slipstart,dur,t.group(1),int(t.group(5))])
        
dfx=pandas.DataFrame.from_dict(out3)
dfx2=dfx.sort_values(by=["ts",'dur'],ascending=[True,False])
js=dfx2.to_dict(orient='records')
jsd=json.dumps(js)
with open(f'/home/binweiyang/trace_view/{jsonfile}.json','w') as w:
    w.write('''
        {
            "traceEvents":
        ''')
    w.write(jsd)
    w.write('''
        }''')

