"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os
import shutil
import time
from itertools import groupby


#
# Escriba la funcion que  genere n copias de los archivos de texto en la
# carpeta files/raw en la carpeta files/input. El nombre de los archivos
# generados debe ser el mismo que el de los archivos originales, pero con
# un sufijo que indique el número de copia. Por ejemplo, si el archivo
# original se llama text0.txt, el archivo generado se llamará text0_1.txt,
# text0_2.txt, etc.
#
def copy_raw_files_to_input_folder(n):
    """Función que copia n veces cada archivo en files/raw a files/input"""
    raw_folder = "files/raw"
    input_folder = "files/input"
    os.makedirs(input_folder, exist_ok=True)

    for filepath in glob.glob(os.path.join(raw_folder, "*.txt")):
        filename = os.path.basename(filepath)
        for i in range(1, n + 1):
            new_filename = f"{filename.rsplit('.', 1)[0]}_{i}.txt"
            new_path = os.path.join(input_folder, new_filename)
            shutil.copy(filepath, new_path)


#
# Escriba la función load_input que recive como parámetro un folder y retorna
# una lista de tuplas donde el primer elemento de cada tupla es el nombre del
# archivo y el segundo es una línea del archivo. La función convierte a tuplas
# todas las lineas de cada uno de los archivos. La función es genérica y debe
# leer todos los archivos de folder entregado como parámetro.
#
# Por ejemplo:
#   [
#     ('text0'.txt', 'Analytics is the discovery, inter ...'),
#     ('text0'.txt', 'in data. Especially valuable in ar...').
#     ...
#     ('text2.txt'. 'hypotheses.')
#   ]
#
def load_input(input_directory):
    """Lee todos los archivos de un directorio y devuelve lista de tuplas (filename, line)"""
    sequence = []
    files = glob.glob(f"{input_directory}/*")
    with fileinput.input(files=files) as f:
        for line in f:
            sequence.append((fileinput.filename(), line))
    return sequence


#
# Escriba la función line_preprocessing que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). Esta función
# realiza el preprocesamiento de las líneas de texto,
#
def line_preprocessing(sequence):
    """Preprocesa las líneas: convierte a minúsculas y elimina caracteres no alfabéticos"""
    import re

    processed = []
    for filename, line in sequence:
        # Convertimos a minúscula y separamos por palabras alfabéticas
        words = re.findall(r"\b\w+\b", line.lower())
        for word in words:
            processed.append((filename, word))
    return processed


#
# Escriba una función llamada maper que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). En este caso,
# la clave es cada palabra y el valor es 1, puesto que se está realizando un
# conteo.
#
#   [
#     ('Analytics', 1),
#     ('is', 1),
#     ...
#   ]
#
def mapper(sequence):
    """Convierte tuplas (archivo, palabra) en (palabra, 1)"""
    return [(word, 1) for _, word in sequence]


#
# Escriba la función shuffle_and_sort que recibe la lista de tuplas entregada
# por el mapper, y retorna una lista con el mismo contenido ordenado por la
# clave.
#
#   [
#     ('Analytics', 1),
#     ('Analytics', 1),
#     ...
#   ]
#
def shuffle_and_sort(sequence):
    """Ordena las tuplas por la clave (palabra)"""
    return sorted(sequence, key=lambda x: x[0])


#
# Escriba la función reducer, la cual recibe el resultado de shuffle_and_sort y
# reduce los valores asociados a cada clave sumandolos. Como resultado, por
# ejemplo, la reducción indica cuantas veces aparece la palabra analytics en el
# texto.
#
def reducer(sequence):
    """Agrupa las palabras y suma los valores"""
    reduced = []
    for key, group in groupby(sequence, key=lambda x: x[0]):
        total = sum(value for _, value in group)
        reduced.append((key, total))
    return reduced


#
# Escriba la función create_ouptput_directory que recibe un nombre de
# directorio y lo crea. Si el directorio existe, lo borra
#
def create_ouptput_directory(output_directory):
    """Crea el directorio de salida, eliminándolo si ya existe"""
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.makedirs(output_directory)


#
# Escriba la función save_output, la cual almacena en un archivo de texto
# llamado part-00000 el resultado del reducer. El archivo debe ser guardado en
# el directorio entregado como parámetro, y que se creo en el paso anterior.
# Adicionalmente, el archivo debe contener una tupla por línea, donde el primer
# elemento es la clave y el segundo el valor. Los elementos de la tupla están
# separados por un tabulador.
#
def save_output(output_directory, sequence):
    """Guarda las tuplas en part-00000 con tabuladores"""
    path = os.path.join(output_directory, "part-00000")
    with open(path, "w", encoding="utf-8") as file:
        for key, value in sequence:
            file.write(f"{key}\t{value}\n")


#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio
# entregado como parámetro.
#
def create_marker(output_directory):
    """Crea el archivo _SUCCESS"""
    path = os.path.join(output_directory, "_SUCCESS")
    with open(path, "w", encoding="utf-8"):
        pass


#
# Escriba la función job, la cual orquesta las funciones anteriores.
#


def run_job(input_directory, output_directory):
    """Orquesta el procesamiento de los archivos"""
    data = load_input(input_directory)
    preprocessed = line_preprocessing(data)
    mapped = mapper(preprocessed)
    shuffled = shuffle_and_sort(mapped)
    reduced = reducer(shuffled)
    create_ouptput_directory(output_directory)
    save_output(output_directory, reduced)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
