DBDIR=db
DBNAME=chain.db
CORPUS=path/to/corpus/file

.PHONY: db

main: pmi langmodel

pmi:
	python pmi.py ${DBDIR}/${DBNAME} ${CORPUS} >${DBDIR}/pmi.bat
	sqlite3 ${DBDIR}/${DBNAME} <${DBDIR}/pmi.bat

langmodel:
	python langmodel.py ${DBDIR}/${DBNAME} ${CORPUS}

clean:
	rm ${DBDIR}/*
