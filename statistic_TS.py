#! python

####################
### DGF 4/7/2013 ###
####################

### per windows ###

import os
import re
import sys
import zipfile
import fileinput
from glob import glob
import csv
import datetime
from datetime import date
import time
import logging
import smtplib
import collections


#########################################
### dichiarazione costanti simboliche ###
#########################################

PATH_HOME = "C:\Python33\Script"
PATH_LOG = PATH_HOME+"\Log"
PATH_ZIP_DAY_DIR = "C:\FTP\controlli"
PATH_ZIP_MONTH_DIR = "C:\FTP\statitt"
PATH_EXTRACT_DAY_DIR = PATH_ZIP_DAY_DIR+"\\Unziped"
PATH_EXTRACT_MONTH_DIR = PATH_ZIP_MONTH_DIR+"\\unziped"
MSG_FROM = "pippo@pippo.it"
RECIPIENT = "pippo1@pippo1.it"
RECIPIENT1 = "pippo2@pippo2.it"
MAILSERVER = "1.1.1.1"

### costanti per gli SLA ###
SLA_FLCAHQ = 5
SLA_FLCAPER_PERC = 5
SLA_FLTSPER = 5

### definizione dell'oggetto logger che serve a gestire i log ###
SYSTEM_DATE=time.strftime("%Y%m%d")
LOG_FILENAME = PATH_LOG+"/statistic-"+SYSTEM_DATE+".log"
logger = logging.getLogger("statistic-1.0.py")
hdlr = logging.FileHandler(LOG_FILENAME)
FORMAT = logging.Formatter('%(asctime)s - [%(levelname)s] %(message)s')
hdlr.setFormatter(FORMAT)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


############################
### definizione funzioni ###
############################

### Faccio l'unzip delle dir contenenti le statistiche ###
def ExtractZipFile(PATH_ZIP, PATH_EXTRACT, DATE):
    for directory in os.listdir(PATH_ZIP):
        if directory.endswith(time.strftime(DATE)+".zip"):
            zip = zipfile.ZipFile(PATH_ZIP+"\\"+directory)
            zip.extractall(PATH_EXTRACT)
            logger.info("ho eseguito l'unzip del seguente file : "+directory)


### Manda mail in caso di necessita' ###
def SendMail(mess):
    FROM = MSG_FROM
    TO = [RECIPIENT,RECIPIENT1]
    SUBJECT = "errore statistiche SLA CAOP2"
    TEXT = mess

    message = """\
From: %s
To: %s
Subject: %s
%s
""" % (FROM, ", ".join(TO), SUBJECT, TEXT)

    ### Send the mail ###
    server = smtplib.SMTP(MAILSERVER)
    server.sendmail(FROM, TO, message)
    server.quit()


### funzione per il controllo del numero totale delle marche emesse nel mese precedente ###
def TimelineMonthControl(PATH_TIMELINE):
    FILES = 0
    TOT_FILE_MONTH = 0
    TOT_FILE_TSR_ATTESI = 0
    NUM_FILE_TSR = 0
    
    for j in os.walk(PATH_TIMELINE):
        FILES += len(j[2])

    END_DATE = LastDayOfMonth(datetime.date(int(TO_YEAR), int(TO_MONTH), 1))

    TOT_FILE_MONTH = int(END_DATE) * 10

    if FILES == TOT_FILE_MONTH:
        logger.info("I file del mese precedente coincidono e sono : "+TOT_FILE_MONTH)
    else:
        TOT_FILE_TSR_ATTESI = TOT_FILE_MONTH / 2

        for path, subdirs, files in os.walk(PATH_TIMELINE):
            for FILENAME in [ f for f in files if re.search('.tsr$', f, re.I)]:
                NUM_FILE_TSR += len(FILENAME[2])
        
        logger.error("file mensili non corrispondenti attesi : "+str(TOT_FILE_MONTH)+", effettivi : "+str(FILES)+" tsr attese : "+str(TOT_FILE_TSR_ATTESI)+" effettive : "+str(NUM_FILE_TSR))
        SendMail("ATTENZIONE ! file mensili non corrispondenti, attesi : "+str(TOT_FILE_MONTH)+", effettivi : "+str(FILES)+" tsr attese : "+str(TOT_FILE_TSR_ATTESI)+" effettive : "+str(NUM_FILE_TSR))
        logger.info("inviata mail di errore")

    print(FILES, NUM_FILE_TSR)


### funzione per il calcolo degli sla giornalieri FLCA_DISP ###
def DayTimeControlSlaFlcadisp(PATH_TIMELINE):
    
    ### Controllo flusso FLCADISP ###
    FNAME_DISP = glob(PATH_TIMELINE+'\\FLCADISP_*.txt')

    for fn in FNAME_DISP:
        with open(fn, 'r', newline='') as f:
            reader = csv.reader(f, delimiter = "|")
            for row in reader:
                if row[3] == 'FECA':
                    feca = row[2]
                elif row[3] == 'DSCA':
                    dsca = row[2]
                    
    logger.info("il valore per il flusso FLCADISP FECA e' : "+feca)
    logger.info("il valore per il flusso FLCADISP FECA e' : "+dsca)
    

### funzione per il calcolo degli sla giornalieri FLCA_HQ ###
def DayTimeControlSlaFlcahq(PATH_TIMELINE):
    
    ### Controllo FLCAHD superamento SLA ###
    cont_hq = 0
    stat_hq = 0
    cont_hq_up = 0
    
    FNAME_HQ = glob(PATH_TIMELINE+'\\FLCAHD_*.txt')
    
    for fnhq in FNAME_HQ:
        with open(fnhq, 'r', newline='') as ff:
            reader_hq = csv.reader(ff, delimiter = "|")
            
            for row_hq in reader_hq:
                ### variabile incrementale per il numero totale di revoche emesse ###
                cont_hq +=1
                
                ### controllo se il valore ha supertato la soglia ###
                if float(row_hq[4]) > 10 :
                    cont_hq_up +=1
                    #logger.error("Il valore ha suparto lo SLA per FLCAHD, ed e' :"+row_hq[4])          
                else:
                    logger.info("il valore per FLCAHD è "+row_hq[4])

    ### verifica se ho superato lo sla ###
    stat_hq =  cont_hq_up * 100 / cont_hq
    
    if stat_hq > SLA_FLCAHQ:
        SendMail("ATTENZIONE !\nil valore FLCAHD ha superato la soglia massima "+str(SLA_FLCAHQ)+" ed e' uguale a "+row_hq[4])
        logger.info("Il numero totale delle revoche e' "+str(cont_hq))
        logger.warning("il valore FLCAHD ha superato la soglia massima "+str(SLA_FLCAHQ)+" ed e' uguale a "+stat_hq)


### funzione per il calcolo degli sla giornalieri FLCA_PER totali ###
def DayTimeControlSlaFlcaperTot(PATH_TIMELINE):

    ### definisco la struttura dove inserirò per ogni flusso il numero massimo delle occorrenze ###
    struct = collections.namedtuple('struct', 'nome totale')

    FNAME_PER = glob(PATH_TIMELINE+'\\FLCAPER_*.txt')

    ### definisco l'oggetto che mi servirà per contare il numero di occorrenze ###
    c = collections.Counter()
    
    for fnper in FNAME_PER:
        with open(fnper, 'r', newline='') as fff:
            reader_per = csv.reader(fff, delimiter = "|")
            
            for row_per in reader_per:
                c[row_per[1]] += 1

    logger.info("Valori per ogni evento FLCAPER : "+str((c.most_common())))

    struct = list(c.most_common())

    ### richiamo le funzioni con i singoli sla ###
    DayTimeControlSlaFlcaperPercent(PATH_TIMELINE, struct)


### funzione per il calcolo degli sla giornalieri FLCA_PER ###
def DayTimeControlSlaFlcaperPercent(PATH_TIMELINE, s):
    ### sort della struttura ###
    s.sort()
    
    ### dichiarazioni variabili locali ###
    cont_per_cacsp = 0
    cont_per_caem = 0
    cont_per_caric = 0
    cont_per_carv = 0
    cont_per_casn = 0
    cont_per_casp = 0
    cont_per_cfcrl = 0
    cont_per_cscrl = 0
    cont_per_notrg = 0
    cont_per_notrv = 0
    fuori_sla_cacsp = 0
    fuori_sla_caem = 0
    fuori_sla_caric = 0
    fuori_sla_carv = 0
    fuori_sla_casn = 0
    fuori_sla_casp = 0
    fuori_sla_cfcrl = 0
    fuori_sla_cscrl = 0
    fuori_sla_notrg = 0
    fuori_sla_notrv = 0
        
    FNAME_PER2 = glob(PATH_TIMELINE+'\\FLCAPER_*.txt')

    for fnper2 in FNAME_PER2:
        with open(fnper2, 'r', newline='') as fff2:
            
            reader_per2 = csv.reader(fff2, delimiter = "|")

            ### calcolo dei fuori SLA ###
            for row_per2 in reader_per2:
                ### CACSP ###
                if row_per2[1] == 'CACSP' and float(row_per2[4]) > 15 :
                    cont_per_cacsp +=1
                    #logger.warning("attenzione il valore di CAEM ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CAEM' and float(row_per2[4]) > 120 :
                    cont_per_caem +=1
                    #logger.warning("attenzione il valore di CAEM ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CARIC' and float(row_per2[4]) > 10:
                    cont_per_caric +=1
                    #logger.warning("attenzione il valore di CARIC ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CARV' and float(row_per2[4]) > 10:
                    cont_per_carv +=1
                    #logger.warning("attenzione il valore di CARIC ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CASN' and float(row_per2[4]) > 10:
                    cont_per_casn +=1
                    #logger.warning("attenzione il valore di CARSN ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CASP' and float(row_per2[4]) > 10:
                    cont_per_casp +=1
                    #logger.warning("attenzione il valore di CARSP ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CFCRL' and float(row_per2[4]) > 4:
                    cont_per_cfcrl +=1
                    #logger.warning("attenzione il valore di CFCRL ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'CSCRL' and float(row_per2[4]) > 4:
                    cont_per_cscrl +=1
                    #logger.warning("attenzione il valore di CSCRL ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'NOTRG' and float(row_per2[4]) > 30:
                    cont_per_notrg +=1
                    #logger.warning("attenzione il valore di NOTRG ha superato la soglia di "+str(row_per2))
                elif row_per2[1] == 'NOTRV' and float(row_per2[4]) > 30:
                    cont_per_notrv +=1
                    #logger.warning("attenzione il valore di NOTRV ha superato la soglia di "+str(row_per2))   

    ### calcolo delle percentuali per i diversi flussi ###
    for j in s:
        if j[0] == "CACSP":
            fuori_sla_cacsp = cont_per_cacsp * 100 / j[1]
            
            if fuori_sla_cacsp > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CACSP SUPERATO!\nil valore e' di : "+str(fuori_sla_cacsp)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CACSP SUPERATO!\nil valore e' di : "+str(fuori_sla_cacsp)+" %")
            else:
                logger.info("CACSP sotto SLA")
                
        elif j[0] == "CAEM":
            fuori_sla_caem = cont_per_caem * 100 / j[1]
            
            if fuori_sla_caem > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CAEM SUPERATO!\nil valore e' di : "+str(fuori_sla_caem)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CAEM SUPERATO!\nil valore e' di : "+str(fuori_sla_caem)+" %")
            else:
                logger.info("CAEM sotto SLA")
    
        elif j[0] == 'CARIC':
            fuori_sla_caric = cont_per_caric * 100 / j[1]
            
            if fuori_sla_caric > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CARIC SUPERATO!\nil valore e' di : "+str(fuori_sla_caric)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CARIC SUPERATO!\nil valore e' di : "+str(fuori_sla_caric)+" %")
            else:
                logger.info("CARIC sotto SLA")
                
        elif j[0] == 'CARV':
            fuori_sla_carv = cont_per_carv * 100 / j[1]
            
            if fuori_sla_carv > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CARV SUPERATO!\nil valore e' di : "+str(fuori_sla_carv)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CARV SUPERATO!\nil valore e' di : "+str(fuori_sla_carv)+" %")
            else:
                logger.info("CARV sotto SLA")

        elif j[0] == 'CASN':
            fuori_sla_casn = cont_per_casn * 100 / j[1]
            
            if fuori_sla_casn > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CASN SUPERATO!\nil valore e' di : "+str(fuori_sla_casn)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CASN SUPERATO!\nil valore e' di : "+str(fuori_sla_casn)+" %")
            else:
                logger.info("CASN sotto SLA")

        elif j[0] == 'CASP':
            fuori_sla_casp = cont_per_casp * 100 / j[1]
            
            if fuori_sla_casp > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CASP SUPERATO!\nil valore e' di : "+str(fuori_sla_casp)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CASP SUPERATO!\nil valore e' di : "+str(fuori_sla_casp)+" %")
            else:
                logger.info("CASP sotto SLA")

        elif j[0] == 'CFCRL':
            fuori_sla_cfcrl = cont_per_cfcrl * 100 / j[1]
            
            if fuori_sla_cfcrl > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CFCRL SUPERATO!\nil valore e' di : "+str(fuori_sla_cfcrl)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CFCRL SUPERATO!\nil valore e' di : "+str(fuori_sla_cfcrl)+" %")
            else:
                logger.info("CFCRL sotto SLA")

        elif j[0] == 'CSCRL':
            fuori_sla_cscrl = cont_per_cscrl * 100 / j[1]
            
            if fuori_sla_cscrl > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_CSCRL SUPERATO!\nil valore e' di : "+str(fuori_sla_cscrl)+" %")
                logger.info("ATTENZIONE SLA flusso FLCAPER_CSCRL SUPERATO!\nil valore e' di : "+str(fuori_sla_cscrl)+" %")
            else:
                logger.info("CSCRL sotto SLA")

        elif j[0] == 'NOTRG':
            fuori_sla_notrg = cont_per_notrg * 100 / j[1]
            
            if fuori_sla_notrg > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_NOTRG SUPERATO!\nil valore e' di : "+str(fuori_sla_notrg)+" %")
                logger.error("ATTENZIONE SLA flusso FLCAPER_NOTRG SUPERATO!\nil valore e' di : "+str(fuori_sla_notrg)+" %")
            else:
                logger.info("NOTRG sotto SLA")

        elif j[0] == 'NOTRV':
            fuori_sla_notrv = cont_per_notrv * 100 / j[1]
            
            if fuori_sla_notrv > SLA_FLCAPER_PERC:
                SendMail("ATTENZIONE SLA flusso FLCAPER_NOTRV SUPERATO!\nil valore e' di : "+str(fuori_sla_notrv)+" %")
                logger.error("ATTENZIONE SLA flusso FLCAPER_NOTRV SUPERATO!\nil valore e' di : "+str(fuori_sla_notrv)+" %")
            else:
                logger.info("NOTRV sotto SLA")


### funzione per il calcolo degli sla giornalieri FLCA_SPER ###
def DayTimeControlSlaFltsperTot(PATH_TIMELINE):
    
    count_sper= 0
    count_sper2 = 0
    
    FNAME_SPER = glob(PATH_TIMELINE+'\\FLTSPER_*.txt')

    for fnsper in FNAME_SPER:
        with open(fnsper, 'r', newline='') as sper:
            
            reader_sper = csv.reader(sper, delimiter = "|")
            
            for row_sper in reader_sper:
                if row_sper[1] == 'CATS1':
                    count_sper = count_sper + int(row_sper[4])
                elif row_sper[1] == 'CATS2':
                    count_sper2 = count_sper2 + int(row_sper[4])

    sla_fltsper = count_sper2 * 100 / count_sper

    if sla_fltsper > SLA_FLTSPER:
        logging.warning("ATTENZIONE SLA flusso FLTSPER superato!\nIl valore e' di : "+str(sla_fltsper))
        SendMail("ATTENZIONE SLA flusso FLTSPER superato!\nIl valore e' di : "+str(sla_fltsper))
            
   
### funzione per il calcolo l'ultima giornata del mese precedente a quello di esecuzione dello script ###       
def LastDayOfMonth(DT):
    END_DATE   = datetime.date(DT.year, DT.month, 1) - datetime.timedelta(days=1)
    return (END_DATE.strftime("%d"))


### calcolo il mese e l'anno precedente a quello di esecuzione dello script ###
def DateMonthBefore(DT):
    DATE_MONTH_BEFORE = datetime.date(DT.year, DT.month, 1) - datetime.timedelta(days=1)
    return(DATE_MONTH_BEFORE.strftime("%Y%m"))


############
### MAIN ###
############

if __name__ == '__main__':

    TO_YEAR = datetime.date.today().strftime("%Y")
    TO_MONTH = datetime.date.today().strftime("%m")

    ### Verifico se è il primo del mese ###  
    if SYSTEM_DATE == time.strftime("%Y%m01"):
        
        DATE_MONTH_BEFORE = DateMonthBefore(datetime.date(int(TO_YEAR), int(TO_MONTH), 1))
               
        ### statistiche da effettuare solo il primo del mese ###
        ExtractZipFile(PATH_ZIP_MONTH_DIR, PATH_EXTRACT_MONTH_DIR, DATE_MONTH_BEFORE)
        TimelineMonthControl(PATH_EXTRACT_MONTH_DIR)

    else:       
        ### statistiche da effettuare tutti i giorni ###
        ExtractZipFile(PATH_ZIP_DAY_DIR, PATH_EXTRACT_DAY_DIR, TO_YEAR+TO_MONTH)
        DayTimeControlSlaFlcadisp(PATH_EXTRACT_DAY_DIR)
        DayTimeControlSlaFlcahq(PATH_EXTRACT_DAY_DIR)
        DayTimeControlSlaFlcaperTot(PATH_EXTRACT_DAY_DIR)
        DayTimeControlSlaFltsperTot(PATH_EXTRACT_DAY_DIR)
