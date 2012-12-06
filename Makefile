CODEDIR=silverberg
SCRIPTSDIR=scripts
PYTHONLINT=pep8 --exclude=cassandra
PYDIRS=${CODEDIR} ${SCRIPTSDIR}
UNITTESTS ?= ${CODEDIR}/test
THRIFT_COMPILER ?= $(shell which thrift)
test:   unit

lint:
    
	${PYTHONLINT} ${PYDIRS}

unit:
	PYTHONPATH=".:${PYTHONPATH}" trial --random 0 ${UNITTESTS}

coverage:
	PYTHONPATH=".:${PYTHONPATH}" coverage run --source=${CODEDIR} --branch `which trial` ${CODEDIR}/test && coverage html -d _trial_coverage --omit="${CODEDIR}/test/*"

thrift:
	${THRIFT_COMPILER} -out silverberg/ --gen py:twisted interface/cassandra.thrift	

clean:
	find . -name '*.pyc' -delete
	find . -name '.coverage' -delete
	find . -name '_trial_coverage' -print0 | xargs rm -rf
	find . -name '_trial_temp' -print0 | xargs rm -rf
	rm -rf dist build *.egg-info