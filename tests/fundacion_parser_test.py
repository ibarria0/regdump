import random
import re
import unittest
from modules import parser
from bs4 import BeautifulSoup,SoupStrainer
from decimal import Decimal
from datetime import datetime
from time import sleep
with open('tests/fundacion_test.html', encoding='latin-1') as f:
    html = f.read()
with open('tests/test_false.html', encoding='latin-1') as f:
    html_false = f.read()
soup = BeautifulSoup(html,'html.parser',parse_only=SoupStrainer('p'),from_encoding='latin-1')
soup_false = BeautifulSoup(html_false,'html.parser',parse_only=SoupStrainer('p'),from_encoding='latin-1')

class TestFundacionParser(unittest.TestCase):
    # Partes no incluidas en el test:
    # Fecha de Escritura
    # Datos 1a. Tasa Unica
    # Fecha de Pago
    # Datos del Diario: Tomo, Asiento
    # y otros que no tienen info

    def setUp(self):
        self.soup = soup

    def test_agente(self):
        self.assertEqual(parser.collect_agente(self.soup),"VARELA CARDENAL Y ASOCIADOS")

    def test_exists(self):
        self.assertFalse(parser.exists(html_false))
        self.assertTrue(parser.exists(html))

    def test_ficha(self):
        self.assertEqual(parser.collect_ficha(self.soup),2372428)

    # modified test_capital to fit fundacion's patrimonio
    def test_patrimonio(self):
        self.assertEqual(parser.collect_patrimonio(self.soup), float(10000.0))

    def test_patrimonio_text(self):
        self.assertEqual(parser.collect_patrimonio_text(self.soup), "EL PATRIMONIO INICIAL DE LA FUNDACION ES DE 10,000 DOLARES AMERICANOS.")

    # modified test_dignatarios to fit fundacion's cargos
    def test_cargos(self):
        self.assertEqual(parser.collect_cargos(self.soup),{'PRESIDENTE':['MICHAEL BORER BIERMANN'], 'TESORERO': ['CAROLINA RAQUEL CONTE AMADO'], 'SECRETARIO': ['TERESITA DEL CARMEN MEDRANO']})

    # modified test_directores to fit fundacion's miembros
    def test_miembros(self):
        self.assertEqual(parser.collect_miembros(self.soup),['MICHAEL BORER BIERMANN', 'TERESITA DEL CARMEN MEDRANO', 'CAROLINA RAQUEL CONTE AMADO'])

    def test_documento(self):
        self.assertEqual(parser.collect_documento(self.soup), 2372428)

    def test_duracion(self):
        self.assertEqual(parser.collect_duracion(self.soup),"PERPETUA")

    def test_escritura(self):
        self.assertEqual(parser.collect_escritura(self.soup), 4218)

    def test_fecha_registro(self):
        self.assertEqual(parser.collect_fecha_registro(self.soup),datetime.strptime("23-04-2013", "%d-%m-%Y" ).date())

    # added test_fundadores for fundacion's different entry
    def test_fundadores(self):
        self.assertEqual(parser.collect_fundadores(self.soup),["CAMILO JOSE AMADO D'ORAZIO"])

    def test_moneda(self):
        self.assertEqual(parser.collect_moneda(self.soup),"DOLARES AMERICANOS.")

    def test_nombre(self):
        self.assertEqual(parser.collect_nombre_fundacion(self.soup),"FUNDACION RAMECA")

    def test_notaria(self):
        self.assertEqual(parser.collect_notaria(self.soup),"NOTARIA OCTAVA DEL CIRCUITO")

    def test_provincia(self):
        self.assertEqual(parser.collect_provincia(self.soup),"PANAMA")

    # modified test_representante_text to fit fundacion's firmante_text
    def test_firmante_text(self):
        self.assertEqual(parser.collect_firmante_text(self.soup),"LA FIRMA CONJUNTA DE CUALQUIERA DE DOS DE LOS MIENBROS DE LA FUNDACIONEN CUALQUIER TRAMITE, ACTO,TRANSACCION O NEGOCIO OBLIGARA A LA FUNDACION")

    def test_status(self):
        self.assertEqual(parser.collect_status(self.soup),"VIGENTE")

    # deleted test_subscriptores because it doesn't exist in fundacion

if __name__ == '__main__':
    unittest.main()
