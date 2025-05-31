# VIES Vat Controller
Programma python che prende in input un file CSV con una lista di PIVA EU e ne controlla la validità
richiamando le **API VIES**. Oltre a presentare i risutlati a video, il programma genera
un report dettagliato in PDF e, se previsto dal file di configurazione, salva i dati in un database
di SQLite Cloud.

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
python.exe .\src\vat_controller.py <file_config>
