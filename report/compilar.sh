#!/bin/bash

# Limpiar archivos auxiliares
./limpiar.sh

# Compilar
pdflatex report.tex
makeglossaries report
pdflatex report.tex
pdflatex report.tex

# Limpiar archivos auxiliares
./limpiar.sh
