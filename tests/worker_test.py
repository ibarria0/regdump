import random
import re
import unittest
from modules import parser
import Worker
from Classes import Sociedad,Persona
from bs4 import BeautifulSoup,SoupStrainer
from datetime import datetime
from random import randint
from time import sleep

class TestWorker(unittest.TestCase):

    def setUp(self):
        self.html = open('tests/test.html', encoding='latin-1')
        self.sociedad = Sociedad('BACON MANAGEMENT INC.',617214)
        self.sociedad.agente = "BUFETE MF & CO."
        self.sociedad.capital = float(10000.0)
        self.sociedad.capital_text = "EL CAPITAL AUTORIZADO DE LA SOCIEDAD ES DE 10,000.00 DOLARES AMERICANOSDIVIDIDO EN 100 ACCIONES, QUE PODRAN SER NOMINATIVAS O AL PORTADOR, DEUN VALOR NOMINAL DE 100.00 DOLARES CADA UNA."
        self.sociedad.dignatarios = {pair[0]:{Persona(v) for v in pair[1]} for pair in {'PRESIDENTE':{'GEORGE ALLEN'}, 'VICE-PRESIDENTE': {'YVETTE ROGERS'},'TESORERO': {'YVETTE ROGERS'}, 'SECRETARIO': {'CARMEN WONG'}, 'SUB-SECRETARIO': {'JAQUELINE ALEXANDER', 'VERNA DE NELSON'}}.items()}
        self.sociedad.directores = {Persona(p) for p in ['GEORGE ALLEN', 'CARMEN WONG', 'YVETTE ROGERS', 'JAQUELINE ALEXANDER', 'VERNA DE NELSON']}
        self.sociedad.duracion = "PERPETUA"
        self.sociedad.fecha_registro = datetime.strptime("23-05-2008", "%d-%m-%Y" ).date()
        self.sociedad.moneda = "DOLARES AMERICANOS."
        self.sociedad.notaria = "NOTARIA OCTAVA DEL CIRCUITO"
        self.sociedad.provincia = "PANAMA"
        self.sociedad.representante_text = "EL REPRESENTANTE LEGAL DE LA SOCIEDAD LO ES EL PRESIDENTE, PUDIENDOTAMBIEN EJERCER ESE CARGO EL TESORERO O EL SECRETARIO EN LAS AUSENCIASDEL PRESIDENTE O CUALQUIER PERSONA QUE LA JUNTA DIRECTIVA DESIGNE CONESE OBJETO."
        self.sociedad.status = "VIGENTE"
        self.sociedad.subscriptores = {Persona(p) for p in ['ENDERS INC.', 'ROCKALL INC.']}
        self.sociedad.asociaciones = dict(list({'directores': self.sociedad.directores, 'subscriptores':self.sociedad.subscriptores}.items()) + list(self.sociedad.dignatarios.items()))
        self.soup = BeautifulSoup(self.html,'html.parser',parse_only=SoupStrainer('p'),from_encoding='latin-1')

    def test_scrape_sociedad_directores(self):
        personas = Worker.scrape_sociedad_directores(self.soup)
        self.assertEqual(personas,self.sociedad.directores)

    def test_scrape_sociedad_subscriptores(self):
        personas = Worker.scrape_sociedad_subscriptores(self.soup)
        self.assertEqual(personas,self.sociedad.subscriptores)

    def test_scrape_sociedad_dignatarios(self):
        personas = Worker.scrape_sociedad_dignatarios(self.soup)
        self.assertEqual(personas,self.sociedad.dignatarios)

    def test_unpack_persona_in_digatarios(self):
        personas = Worker.unpack_personas_in_dignatarios(self.sociedad.dignatarios)
        self.assertEqual(personas,self.sociedad.directores)

    def test_scrape_sociedad_data(self):
        new_sociedad = Worker.scrape_sociedad_data(self.soup)
        self.assertEqual(new_sociedad,self.sociedad)
        for key in new_sociedad.__dict__.keys():
            if key != '_sa_instance_state':
                self.assertEqual(new_sociedad[key],self.sociedad[key])

    def test_scrape_personas(self):
        personas,asociaciones = Worker.scrape_personas(self.soup)
        self.assertEqual(personas,self.sociedad.directores.union(self.sociedad.subscriptores))
        self.assertEqual(asociaciones,self.sociedad.asociaciones)

    def test_resolve_asociaciones(self):
        personas = self.sociedad.directores.union(self.sociedad.subscriptores)
        personas_with_id = set()
        for persona in personas:
            p = Persona(persona.nombre)
            p.id = randint(1,10000)
            personas_with_id.add(p)
        asociaciones = dict(self.sociedad.asociaciones)
        asociaciones = Worker.resolve_asociaciones(personas_with_id,asociaciones)
        self.assertEqual(self.sociedad.asociaciones,asociaciones)
        self.assertTrue(all([p.id is not None for sublist in asociaciones.values() for p in sublist]))



if __name__ == '__main__':
    unittest.main()
