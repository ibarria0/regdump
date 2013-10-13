import random
import re
import unittest
from modules import parser
from bs4 import BeautifulSoup,SoupStrainer
from decimal import Decimal
from datetime import datetime
from time import sleep

class TestParser(unittest.TestCase):

    def setUp(self):
        self.html = open('tests/test.html').read()
        self.soup = BeautifulSoup(self.html,'html.parser',parse_only=SoupStrainer('p'))

    def test_agente(self):
        self.assertEqual(parser.collect_agente(self.soup),"BUFETE MF & CO.")

    def test_capital(self):
        self.assertEqual(parser.collect_capital(self.soup),float(10000.0))

    def test_capital_text(self):
        self.assertEqual(parser.collect_capital_text(self.soup),"EL CAPITAL AUTORIZADO DE LA SOCIEDAD ES DE 10,000.00 DOLARES AMERICANOSDIVIDIDO EN 100 ACCIONES, QUE PODRAN SER NOMINATIVAS O AL PORTADOR, DEUN VALOR NOMINAL DE 100.00 DOLARES CADA UNA.")

    def test_dignatarios(self):
        self.assertEqual(parser.collect_dignatarios(self.soup),[(u'PRESIDENTE', u'GEORGE ALLEN'), (u'VICE-PRESIDENTE', u'YVETTE ROGERS'), (u'TESORERO', u'YVETTE ROGERS'), (u'SECRETARIO', u'CARMEN WONG'), (u'SUB-SECRETARIO', u'JAQUELINE ALEXANDER'), (u'SUB-SECRETARIO', u'VERNA DE NELSON')])

    def test_directores(self):
        self.assertEqual(parser.collect_directores(self.soup),[u'GEORGE ALLEN', u'CARMEN WONG', u'YVETTE ROGERS', u'JAQUELINE ALEXANDER', u'VERNA DE NELSON'])

    def test_duracion(self):
        self.assertEqual(parser.collect_duracion(self.soup),"PERPETUA")

    def test_fecha_registro(self):
        self.assertEqual(parser.collect_fecha_registro(self.soup),datetime.strptime("23-05-2008", "%d-%m-%Y" ).date())

    def test_moneda(self):
        self.assertEqual(parser.collect_moneda(self.soup),"DOLARES AMERICANOS.")

    def test_nombre(self):
        self.assertEqual(parser.collect_nombre(self.soup),"BACON MANAGEMENT INC.")

    def test_notaria(self):
        self.assertEqual(parser.collect_notaria(self.soup),"NOTARIA OCTAVA DEL CIRCUITO")

    def test_provincia(self):
        self.assertEqual(parser.collect_provincia(self.soup),"PANAMA")

    def test_representante_text(self):
        self.assertEqual(parser.collect_representante_text(self.soup),"EL REPRESENTANTE LEGAL DE LA SOCIEDAD LO ES EL PRESIDENTE, PUDIENDOTAMBIEN EJERCER ESE CARGO EL TESORERO O EL SECRETARIO EN LAS AUSENCIASDEL PRESIDENTE O CUALQUIER PERSONA QUE LA JUNTA DIRECTIVA DESIGNE CONESE OBJETO.")

    def test_status(self):
        self.assertEqual(parser.collect_status(self.soup),"VIGENTE")

    def test_subscriptores(self):
        self.assertEqual(parser.collect_subscriptores(self.soup),[u'ENDERS INC.', u'ROCKALL INC.'])


if __name__ == '__main__':
    unittest.main()
