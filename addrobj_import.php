<?php

class import
{

    public $queries         = 0;
    public $file = '/AS_ADDROBJ.XML';
    public $pdo;
    public $inserLast     = false;
    public $package         = [];
    public $fields = ['AOID', 'AOGUID', 'PARENTGUID', 'FORMALNAME', 'OFFNAME', 'SHORTNAME', 'AOLEVEL', 'REGIONCODE', 'AREACODE', 'AUTOCODE', 'CITYCODE', 'CTARCODE', 'PLACECODE', 'STREETCODE', 'EXTRCODE', 'SEXTCODE', 'PLAINCODE', 'CODE', 'CURRSTATUS', 'ACTSTATUS', 'LIVESTATUS', 'CENTSTATUS', 'OPERSTATUS', 'IFNSFL', 'IFNSUL', 'OKATO', 'OKTMO', 'POSTALCODE', 'STARTDATE', 'ENDDATE', 'UPDATEDATE', 'NEXTID', 'NORMDOC', 'TERRIFNSFL', 'TERRIFNSUL', 'PREVID'];
    public $insertString = '';
    
    public function getConnect()
    {
        //Обновляем соединение каждые 10000
        if(!is_null($this->pdo) && $this->queries % 10000 == 0)
        {
            $this->pdo = null;
            echo "\n", $this->queries;
        }
        if($this->queries % 1000 == 0) echo '.';
        if(is_null($this->pdo))
        {
            $this->pdo = new PDO('mysql:host=localhost;dbname=;charset=UTF8', '', '', array(PDO::MYSQL_ATTR_INIT_COMMAND=>"SET NAMES utf8"));
        }
    }

    public function insert($values)
    {
        $this->queries++;
        $this->getConnect();
        if(!$this->inserLast) $this->package[] = $values;
        if(count($this->package) >= 50 || $this->inserLast)
        {
            $sql = 'INSERT INTO `addrobj` (' . $this->insertString . ') VALUES ';

            $lines = [];
            foreach($this->package as $row)
            {
                $param = array();
                foreach($this->fields as $field)
                {
                    $param[] = "'" . (isset($row[$field]) ? $row[$field] : '') . "'";
                    
                }
                $lines[] = '(' . implode(', ', $param) . ')';
            }
            
            $command         = $this->pdo->prepare($sql . implode(', ', $lines));
            $command->execute();
            $this->package     = [];
        }
    }
    
    public function fieldsForInsert()
    {
        $a = [];
        foreach($this->fields as $field)
        {
            $a[] = '`' . $field . '`';
        }
        $this->insertString = implode(', ', $a);
    }

    public function run()
    {
        $this->fieldsForInsert();
        $reader = new XMLReader();
        $reader->open(__DIR__ . $this->file);
        while($reader->read())
        {
            switch($reader->nodeType)
            {
                case (XMLREADER::ELEMENT):
                    if($reader->localName == "AddressObjects")
                    {
                        while($reader->read())
                        {
                            if($reader->nodeType == XMLREADER::ELEMENT)
                            {
                                if($reader->localName == "Object")
                                {
                                    $attes = [];
                                    while($reader->moveToNextAttribute())
                                    {
                                        $attes[$reader->localName] = $reader->value;
                                    }
                                    $this->insert($attes);
                                }
                            }
                        }
                    }
            }
        }
        $this->inserLast = true;
        $this->insert([]);
    }

}
$class = new import;
$class->run();
