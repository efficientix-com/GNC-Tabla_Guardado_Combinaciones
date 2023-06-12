/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @name tkio_guardado_combinaciones_mr_v2
 * @version 2.0
 * @author Dylan Mendoza <dylan.mendoza@freebug.mx>
 * @summary Map para el registro de datos de la busqueda guarada "AGCH Guardado combinaciones mes DEB CRED"
 * @copyright Tekiio México 2023
 * 
 * Client              -> GNC SB2
 * Last modification   -> 03/04/2023
 * Modified by         -> Dylan Mendoza <dylan.mendoza@freebug.mx>
 * Script in NS        -> Tkio - Guardado de Combinaciones <customscript_tkio_guardado_combinaciones>
 */
define(['N/search','N/record','N/log','N/runtime', 'N/format', 'N/file'],
    
(search,record,log,runtime, format, file) => {
    /**
     * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
     * @param {Object} inputContext
     * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {Object} inputContext.ObjectRef - Object that references the input data
     * @typedef {Object} ObjectRef
     * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
     * @property {string} ObjectRef.type - Type of the record instance that contains the input data
     * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
     * @since 2015.2
     */

    const getInputData = (inputContext) => {

       try{
           var dataFiel = ''
           var parameter_search = runtime.getCurrentScript().getParameter({name: "custscript_tkio_gua_combinaciones_search"});
           log.debug({title:'Search Parameter', details:parameter_search});
           var datareturn = [];
           var dataConcatSearch = [];
           var dataSearch = [];
           // Busqueda de Registros en Tabla de Artus
           var searchTable = search.create({
               type: "customrecord_tkio_guardado_combinaciones",
               filters:
               [
                  ["isinactive","is","F"]
               ],
               columns:
               [
                  search.createColumn({
                     name: "id",
                     sort: search.Sort.ASC,
                     label: "ID"
                  }),
                  search.createColumn({name: "custrecord_tkio_gua_com_periodo", label: "Periodo"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_subsidiaria", label: "Subsidiaria Compañía"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_cr", label: "CR"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_mayor", label: "Mayor"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_subcuenta", label: "Subcuenta"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_intercom", label: "Intercompañía"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_adicional", label: "Adicional"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_importe_periodo", label: "Importe en Periodo"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_debito", label: "Débito"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_credito", label: "Credito"}),
                  search.createColumn({name: "custrecord_tkio_gua_com_concatenado", label: "Concatenado"})
               ]
           });
           var myTableData = searchTable.runPaged({
               pageSize: 1000
           });
           var countTable = myTableData.count;
           log.debug({title:'countTable', details:countTable});
           if (countTable) {
               myTableData.pageRanges.forEach(function(pageRange) {
                   var myPage = myTableData.fetch({index: pageRange.index});
                   myPage.data.forEach(function(result) {
                       var id = result.getValue({
                           name: "id",
                           sort: search.Sort.ASC,
                           label: "ID"
                       });
                       var periodo = result.getValue({name: 'custrecord_tkio_gua_com_periodo'});
                       var subsi = result.getValue({name: 'custrecord_tkio_gua_com_subsidiaria'});
                       var cr = result.getValue({name: 'custrecord_tkio_gua_com_cr'});
                       var mayor = result.getValue({name: 'custrecord_tkio_gua_com_mayor'});
                       var subcuenta = result.getValue({name: 'custrecord_tkio_gua_com_subcuenta'});
                       var intercom = result.getValue({name: 'custrecord_tkio_gua_com_intercom'});
                       var adicional = result.getValue({name: 'custrecord_tkio_gua_com_adicional'});
                       var impor_periodo = result.getValue({name: 'custrecord_tkio_gua_com_importe_periodo'});
                       var deb = result.getValue({name: 'custrecord_tkio_gua_com_debito'});
                       var cred = result.getValue({name: 'custrecord_tkio_gua_com_credito'});
                       var concat = result.getValue({name: 'custrecord_tkio_gua_com_concatenado'});
                       datareturn.push({
                           id: id,
                           periodo: periodo,
                           subsidiaria: subsi,
                           cr:cr,
                           mayor: mayor,
                           subcuenta: subcuenta,
                           intercom: intercom,
                           adicional: adicional,
                           importe_periodo: impor_periodo,
                           debito: deb,
                           credito: cred,
                           concatenado: concat,
                           clear: true
                       });
                       dataConcatSearch.push(concat);
                       dataSearch.push({
                           concatenado: concat,
                           periodo: periodo
                       });
                   });
               });
           }
           // Busqueda de resultados de la busqueda parametro
           var busqueda=search.load({
               id: parameter_search
           });
           var myPagedData = busqueda.runPaged({
               pageSize: 1000
           });
           log.debug({title:'dataReturn.Count_TablaArtus', details:datareturn.length});
           log.debug({title:'myPagedDataCount', details:myPagedData.count});
           var arrayAux = []
           myPagedData.pageRanges.forEach(function(pageRange) {
               var myPage = myPagedData.fetch({index: pageRange.index});
               myPage.data.forEach(function(result){
                   var concatenado=result.getValue({
                       name: "formulatext",
                       summary: "GROUP",
                       formula: "(CASE WHEN {subsidiary.custrecordnumerocompania} is NULL THEN '00' ELSE {subsidiary.custrecordnumerocompania} END)||'-'||(CASE WHEN {department.custrecordnumcr} is NULL THEN '000000' ELSE {department.custrecordnumcr} END)||'-'||(CASE WHEN {account.custrecord_segmentomayor} is NULL THEN '0000' ELSE {account.custrecord_segmentomayor} END)||'-'||(CASE WHEN {account.custrecordsegmento_menor} is NULL THEN '0000' ELSE {account.custrecordsegmento_menor} END)||'-'||(LPAD(CASE WHEN (CASE WHEN {line}=0 THEN {csegefxintercompani} ELSE {line.csegefxintercompani} END) is NULL THEN '00' ELSE (CASE WHEN {line}=0 THEN {csegefxintercompani} ELSE {line.csegefxintercompani} END) END,2))||'-'||(CASE WHEN {class.custrecordefx_id_segmentoadicional} is NULL THEN '0000' ELSE {class.custrecordefx_id_segmentoadicional} END)",
                       label: "Concatenado"
                   });
                   var arrayConcatenado=concatenado.split("-");
                   var periodo=result.getText({
                       name: "postingperiod",
                       summary: "GROUP",
                       sort: search.Sort.DESC,
                       label: "Período"
                   });
                   var datocompare = concatenado + '-' + periodo;
                   if (arrayAux.indexOf(datocompare) == -1) {
                       arrayAux.push(datocompare);
                   }else{
                       log.error({title:'Duplicado', details:{conca: datocompare, pos: arrayAux.indexOf(datocompare)}});
                   }
                   var subsidiaria=arrayConcatenado[0];
                   var cr=arrayConcatenado[1];
                   var mayor=arrayConcatenado[2];
                   var subcuenta=arrayConcatenado[3];
                   var intercom=arrayConcatenado[4];
                   var adicional=arrayConcatenado[5];
                   var importe_periodo=result.getValue({
                       name: "formulacurrency",
                       summary: "SUM",
                       formula: "(CASE WHEN {debitamount} IS NULL THEN 0 ELSE {debitamount} END)-(CASE WHEN {creditamount} IS NULL THEN 0 ELSE {creditamount} END)",
                       label: "Importe en periodo"
                   });
                   var debito=result.getValue({
                       name: "debitamount",
                       summary: "SUM",
                       label: "Debito"
                   });
                   var credito=result.getValue({
                       name: "creditamount",
                       summary: "SUM",
                       label: "Credito"
                   });
                   var dataInsert = {
                       periodo:periodo,
                       subsidiaria:subsidiaria,
                       cr:cr,
                       mayor:mayor,
                       subcuenta:subcuenta,
                       intercom:intercom,
                       adicional:adicional,
                       importe_periodo:importe_periodo,
                       debito:debito,
                       credito:credito,
                       concatenado:concatenado
                   }
                   datareturn.push(dataInsert);
                   var dataInsertText = JSON.stringify(dataInsert);
                   dataFiel += dataInsertText;
               });
           });
           log.debug({title:'dataReturn.Count_Tabla+Busqueda', details:datareturn.length});
           var fileObj = file.create({
               name: 'DataReturn.txt',
               fileType: file.Type.PLAINTEXT,
               contents: dataFiel
           });
           // fileObj.folder = 6364; // folder en RP
           // var saveData = fileObj.save();
           return datareturn;
       }catch(e){
           log.error({
               title: "getInputData",
               details: e
           })
       }
    }

   /**
     * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
     * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
     * context.
     * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
     *     is provided automatically based on the results of the getInputData stage.
     * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
     *     function on the current key-value pair
     * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
     *     pair
     * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {string} mapContext.key - Key to be processed during the map stage
     * @param {string} mapContext.value - Value to be processed during the map stage
     * @since 2015.2
     */
   const caracteres = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'];
   var posicionActual = 0;  // valor en posición AAAAAAX
   var posicionActual1 = 0; // valor en posición AAAAAXA
   var posicionActual2 = 0; // valor en posición AAAAXAA
   var posicionActual3 = 0; // valor en posición AAAXAAA
   var posicionActual4 = 0; // valor en posición AAXAAAA
   var posicionActual5 = 0; // valor en posición AXAAAAA
   var posicionActual6 = 0; // valor en posición XAAAAAA
   var indiceConcat = '';
   const map = (mapContext) => {
       try{
           var datos=JSON.parse(mapContext.value);
           var indice =mapContext.key*1;
           indiceConcat += caracteres[posicionActual6];
           indiceConcat += caracteres[posicionActual5];
           indiceConcat += caracteres[posicionActual4];
           indiceConcat += caracteres[posicionActual3];
           indiceConcat += caracteres[posicionActual2];
           indiceConcat += caracteres[posicionActual1];
           indiceConcat += caracteres[posicionActual];
           posicionActual = posicionActual + 1;
           if (posicionActual >= caracteres.length) {
               posicionActual = 0;
               posicionActual1 = posicionActual1 + 1;
           }
           if (posicionActual1 >= caracteres.length) {
               posicionActual1 = 0;
               posicionActual2 = posicionActual2 + 1;
           }
           if (posicionActual2 >= caracteres.length) {
               posicionActual2 = 0;
               posicionActual3 = posicionActual3 + 1;
           }
           if (posicionActual3 >= caracteres.length) {
               posicionActual3 = 0;
               posicionActual4 = posicionActual4 + 1;
           }
           if (posicionActual4 >= caracteres.length) {
               posicionActual4 = 0;
               posicionActual5 = posicionActual5 + 1;
           }
           if (posicionActual5 >= caracteres.length) {
               posicionActual5 = 0;
               posicionActual6 = posicionActual6 + 1;
           }
           if (indiceConcat != '') {
               indice = indiceConcat;
           }
           indiceConcat = '';
           mapContext.write({
               key:indice,
               value:datos
           });
        }catch(e){
           log.error({
               title: "map",
               details: e
           });
       }
   }

    /**
     * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
     * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
     * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
     *     provided automatically based on the results of the map stage.
     * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
     *     reduce function on the current group
     * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
     * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {string} reduceContext.key - Key to be processed during the reduce stage
     * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
     *     for processing
     * @since 2015.2
     */
    const reduce = (reduceContext) => {
       try {
           var data = JSON.parse(reduceContext.values);
           log.audit({title:'Data: ' + reduceContext.key, details:data});
           if (data.concatenado.includes('08-390018-1112-0204')) {
               log.error({title:'Encontrado', details:data});
           }
           if (data.clear) {
               record.submitFields({
                   type: 'customrecord_tkio_guardado_combinaciones',
                   id: data.id,
                   values: {
                      'custrecord_tkio_gua_com_debito' : 0.0,
                      'custrecord_tkio_gua_com_credito' : 0.0,
                      'isinactive': true
                   }
               });
               log.debug({title:'Dato limpio id: ' + data.id, details:data});
           }else{
               var periodo = data.periodo;
               var concatenado = data.concatenado;
               var arrayConcatenado=concatenado.split("-");
               var subsidiaria=arrayConcatenado[0];
               var cr=arrayConcatenado[1];
               var mayor=arrayConcatenado[2];
               var subcuenta=arrayConcatenado[3];
               var intercom=arrayConcatenado[4];
               var adicional=arrayConcatenado[5];
               var importe_periodo=data.importe_periodo;
               var debito=data.debito;
               var credito=data.credito;
               var fecha=new Date();
               //busqueda de lista registro
               var busqueda_combinaciones = search.create({
                   type: "customrecord_tkio_guardado_combinaciones",
                   filters:[ 
                       ["custrecord_tkio_gua_com_periodo","is",periodo],
                       "AND",
                       ["custrecord_tkio_gua_com_concatenado","is",concatenado]
                   ],
                   columns:  [
                       search.createColumn({
                       name: "internalid",
                       label: "ID interno"
                       }),
                       search.createColumn({name: "custrecord_tkio_gua_com_periodo", label: "Periodo"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_subsidiaria", label: "Subsidiaria"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_cr", label: "CR"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_mayor", label: "Mayor"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_subcuenta", label: "Subcuenta"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_adicional", label: "Adicional"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_intercom", label: "Intercompañía"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_debito", label: "Débito"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_credito", label: "Credito"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_importe_periodo", label: "Importe en Periodo"}),
                       search.createColumn({name: "custrecord_tkio_gua_com_concatenado", label: "Concatenado"})
                   ]
                   
               });
               var searchResultCount = busqueda_combinaciones.runPaged().count;
               if(searchResultCount>0){
                   var id_transaccion=0, per_transaction, subsidiaria_transaction,cr_transaction,mayor_transaction,subc_transaction,intercom_transaction,adicional_transaction ,deb_transaction, cred_transaction, imper_transaction, concat_transaction;
                   busqueda_combinaciones.run().each(function(result){
                       id_transaccion=result.getValue({name:'internalid'});
                       subsidiaria_transaction=result.getValue({name:'custrecord_tkio_gua_com_subsidiaria'});
                       cr_transaction=result.getValue({name:'custrecord_tkio_gua_com_cr'});
                       mayor_transaction=result.getValue({name:'custrecord_tkio_gua_com_mayor'});
                       subc_transaction=result.getValue({name:'custrecord_tkio_gua_com_subcuenta'});
                       adicional_transaction=result.getValue({name:'custrecord_tkio_gua_com_adicional'});
                       intercom_transaction=result.getValue({name:'custrecord_tkio_gua_com_intercom'});
                       per_transaction=result.getValue({name:'custrecord_tkio_gua_com_periodo'}); //periodo
                       deb_transaction=result.getValue({name:'custrecord_tkio_gua_com_debito'});
                       cred_transaction=result.getValue({name:'custrecord_tkio_gua_com_credito'});
                       imper_transaction=result.getValue({name:'custrecord_tkio_gua_com_importe_periodo'});
                       concat_transaction=result.getValue({name:'custrecord_tkio_gua_com_concatenado'});
                       if(imper_transaction!=importe_periodo || deb_transaction!=debito || cred_transaction!=credito){
                           updateRecord(id_transaccion, importe_periodo, debito, credito, fecha, data);
                           return false;
                       }
                       return true;
                   });
               }else{
                   //crea nuevo
                   var datos={
                       periodo:periodo,
                       subsidiaria:subsidiaria,
                       cr:cr,
                       mayor:mayor,
                       subcuenta:subcuenta,
                       intercom:intercom,
                       adicional:adicional,
                       importe_periodo:importe_periodo,
                       debito:debito,
                       credito:credito,
                       concatenado:concatenado
                   };
                   createNewRecord(datos, fecha);
               }
           }
       } catch (error) {
           log.error({title:'Reduce error', details:error});
       }
    }

    function updateRecord(idRec, imper_transaction, deb_transaction, cred_transaction, fecha, data) {
        try {
            var objRecord = record.load({
                type: 'customrecord_tkio_guardado_combinaciones',
                id: idRec
            });
            objRecord.setValue({
                fieldId: 'custrecord_tkio_gua_com_importe_periodo',
                value: imper_transaction
            });
            objRecord.setValue({
                fieldId: 'custrecord_tkio_gua_com_debito',
                value: deb_transaction
            });
            objRecord.setValue({
                fieldId: 'custrecord_tkio_gua_com_credito',
                value: cred_transaction
            });
            objRecord.setValue({
                fieldId: 'custrecord_tkio_gua_com_fechamod',
                value: fecha
            });
            objRecord.setValue({
               fieldId: 'isinactive',
               value: false
           });
            var updRec = objRecord.save({
                enableSourcing: true,
                ignoreMandatoryFields: true
            });
            log.audit({title:'updRec', details: {msg : "Registro actualizado id: " + updRec, data: data}});
            return updRec;
        } catch (e) {
            log.error({title:'updateRecord', details:e});
        }
    }

    function createNewRecord(data, fecha) {
        try {
            var newRecord=record.create({
                type: "customrecord_tkio_guardado_combinaciones",
                isDynamic: true
            });

            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_periodo',
                value:data.periodo
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_subsidiaria',
                value:data.subsidiaria
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_cr',
                value:data.cr
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_mayor',
                value:data.mayor
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_subcuenta',
                value:data.subcuenta
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_intercom',
                value:data.intercom
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_adicional',
                value:data.adicional
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_importe_periodo',
                value:data.importe_periodo
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_debito',
                value:data.debito
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_credito',
                value:data.credito
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_concatenado',
                value:data.concatenado
            })
            newRecord.setValue({
                fieldId:'custrecord_tkio_gua_com_fechamod',
                value:fecha
            })

            var saveRecord = newRecord.save({
                enableSourcing: true,
                ignoreMandatoryFields: true
            });
           log.debug({title:'NewRecord', details: {msg :"Registro creado id: " + saveRecord, data: data}});
            return saveRecord;
        } catch (e) {
            log.error({title:'createNewData', details:e});
        }
    }


    /**
     * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
     * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
     * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
     * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
     *     script
     * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
     * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
     * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
     * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
     *     script
     * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
     * @param {Object} summaryContext.inputSummary - Statistics about the input stage
     * @param {Object} summaryContext.mapSummary - Statistics about the map stage
     * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
     * @since 2015.2
     */
    const summarize = (summaryContext) => {
        try{
           log.debug({title:'Sumarize', details:'Sumarize'});
        }catch(e){
            log.error({
                title: "summarize",
                details: e
            })
        }
    }

    return {getInputData, map, reduce, summarize}

});