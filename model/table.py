
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import sqlite3

Base = declarative_base()


class OutputVariable(Base):

    __tablename__ = 'output_variable'
    id = Column(Integer, primary_key=True)
    variable_name = Column(String)
    display_name = Column(String)
    domain = Column(String)
    variable_group = relationship(
        "VariableGroup", back_populates="output_variable")

    def get_variable(self):
        # go over variable grouping
        # aggregate with all groupings
        pass


class VariableGrouping(Base):

    __tablename__ = 'variable_grouping'
    id = Column(Integer, primary_key=True)
    variable_id = Column(Integer, ForeignKey("output_variable.id"))
    instance = Column(String)
    category_option_combo = Column(String)
    output_variable = relationship(
        "OutputVariable", back_populates="variable_group")


engine = create_engine('sqlite:///:memory:', echo=True)

Base.metadata.create_all(engine)


ov1 = OutputVariable(variable_name='bcg_u1',
                     display_name='BCG under 1 Years Old All', domain='EPI')

grouping1 = VariableGrouping(
    instance='new', category_option_combo='<1Y, Male, Static')
grouping2 = VariableGrouping(
    instance='old', category_option_combo='<1Y, Female, Static')
grouping3 = VariableGrouping(
    instance='new', category_option_combo='<1Y, Male2, Static')
grouping4 = VariableGrouping(
    instance='new', category_option_combo='<1Y, Male3, Static')

ov1.variable_group.append(grouping1, grouping2, grouping3, grouping4)


session = sessionmaker(bind=engine)


session.add(ov1)

session.commit()

print('Done')


for var in OutputVariable().session.all():
    var.get_variable()

# 'categoryOptionCombo'


# output_variable
# 1, bcg_u1, EPI


# variable_grouping
# 1, 1, new, <UY, Male, Static
# 2, 1, new, <1Y, Female, Static
# 3, 1, old, <1Y,


# 1, 1~bcg_u1, <1U, Male, new
