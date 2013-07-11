PWD = $(shell pwd)
CODEDIR=silverberg
SCRIPTSDIR=scripts
PYDIRS=${CODEDIR} ${SCRIPTSDIR}
UNITTESTS ?= ${CODEDIR}/test
THRIFT_COMPILER ?= $(shell which thrift)
DOCDIR=doc

test: unit

lint:
	find ${PYDIRS} -not -path '*/cassandra/*' -and -name '*.py' | xargs pyflakes
	pep8 --exclude=cassandra --max-line-length=105 ${PYDIRS}

unit:
	PYTHONPATH=".:${PYTHONPATH}" trial --random 0 ${UNITTESTS}

coverage:
	PYTHONPATH=".:${PYTHONPATH}" coverage run `which trial` silverberg; coverage html -d _trial_coverage

thrift:
	${THRIFT_COMPILER} -out silverberg/ --gen py:twisted interface/cassandra.thrift

docs: cleandocs
	cp -r ${DOCDIR} _builddoc
	sphinx-apidoc -F -T -o _builddoc ${CODEDIR} ${PWD}/${CODEDIR}/cassandra ${PWD}/${CODEDIR}/test
	PYTHONPATH=".:${PYTHONPATH}" sphinx-build -b html _builddoc htmldoc
	rm -rf _builddoc

cleandocs:
	rm -rf _builddoc
	rm -rf htmldoc

clean: cleandocs
	find . -name '*.pyc' -delete
	find . -name '.coverage' -delete
	find . -name '_trial_coverage' -print0 | xargs rm -rf
	find . -name '_trial_temp' -print0 | xargs rm -rf
	rm -rf dist build *.egg-info
