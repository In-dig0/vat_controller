# VIES Vat Controller
Programma python che prende in input una lista di PIVA EU (file CSV) e ne controlla la validità
richiamando le **API VIES**. Oltre a presentare i risutlati a video, il programma genera
un report dettagliato in PDF e, se previsto dal file di configurazione, salva i dati in un database
di SQLite Cloud.

## API VIES
Il sito della [API VIES](https://ec.europa.eu/taxation_customs/vies/#/technical-information) fornisce la documentazione per l'utilizzo delle API messe a dispozione 

Il programma python utilizza i servizi SOAP, con i realtivi WSDL endpoint:
- [Check Vat Service](https://ec.europa.eu/taxation_customs/vies/services/checkVatService.wsdl): to verify the validity of a VAT number;
- [Check VIES status](https://ec.europa.eu/taxation_customs/vies/checkStatusService.wsdl): to retrieve the current status of the application and status of each member state service. 


## INPUT
Il programma prevede la lettura di file CSV da un cartella sorgente. 
Il tracciato del file CSV (separatore ";") è il seguente:
1) Descrizione partner
2) Codice ISO paese
2) Codice PIVA

Esempio:
COD CLIENTE+RAGIONE SOCIALE;ISO;VAT  
07080436    |IPH FRANCE;FR;00353970262

## OUTPUT
Il programma genera un report PDF con l'esito dettagliato della validità del controllo.

## SINTASSI
Sintassi da CLI:
```bash
python.exe .\src\vat_controller.py <file_config>
