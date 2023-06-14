/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @name tkio_guardado_combinaciones_mr
 * @version 1.0
 * @author Dylan Mendoza <dylan.mendoza@tekiio.mx>
 * @summary Map para el registro de datos de la busqueda guarada "AGCH Guardado combinaciones mes DEB CRED"
 * @copyright Tekiio México 2022
 * 
 * Client              -> GNC SB2
 * Last modification   -> 30/04/2023
 * Modified by         -> Dylan Mendoza <dylan.mendoza@tekiio.mx>
 * Script in NS        -> Tkio - Guardado de Combinaciones <customscript_tkio_guardado_combinaciones>
 */
define(['N/search','N/record','N/log','N/runtime', 'N/task', 'N/file'],
    
(search,record,log,runtime, task, file) => {
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
            var parameter_search = runtime.getCurrentScript().getParameter({name: "custscript_tkio_gua_combinaciones_search"});
            log.audit({title:'Busqueda guarada', details:parameter_search});
            var busqueda=search.load({
               id: parameter_search
            });
            return busqueda;
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

    const map = (mapContext) => {
        try{
            var datos=JSON.parse(mapContext.value);
            mapContext.write({
                key:mapContext.key,
                value:datos
            });
        }catch(e){
            log.error({
                title: "map",
                details: e
            })
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
        try{
           
           var data = JSON.parse(reduceContext.values);
           var periodo = data.values["GROUP(postingperiod)"].text;
           var concatenado = data.values["GROUP(formulatext)"];
           //log.debug({title:'Valores', details:{concatenado: concatenado, periodo: periodo}});
           var arrayConcatenado=concatenado.split("-");
           var subsidiaria=arrayConcatenado[0];
           var cr=arrayConcatenado[1];
           var mayor=arrayConcatenado[2];
           var subcuenta=arrayConcatenado[3];
           var intercom=arrayConcatenado[4];
           var adicional=arrayConcatenado[5];

           var importe_periodo=data.values["SUM(formulacurrency)"];
           var debito=data.values["SUM(debitamount)"];
           var credito=data.values["SUM(creditamount)"];

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
                       updateRecord(id_transaccion, importe_periodo,debito, credito, fecha);
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
        }catch(e){
            log.error({
                title: "reduce",
                details: e
            })
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
            log.debug({title:'Fin del proceso', details:'summarize'});
            var parameter_fin = runtime.getCurrentScript().getParameter({name: "custscript_tkio_fin_ciclo"});
            log.debug({title:'Es fin de ciclo?', details:parameter_fin});
            if (parameter_fin == false) {
                log.audit({title:'Se ejecuta la segunda vuelta', details:'EJECUTANDO'});
                var mrTask = task.create({taskType: task.TaskType.MAP_REDUCE});
                mrTask.scriptId = 'customscript_tkio_guardado_combinaciones';
                mrTask.deploymentId = 'customdeploy_tkio_guardado_combin_2';
                var mrTaskId = mrTask.submit();
            }
        }catch(e){
            log.error({
                title: "summarize",
                details: e
            })
        }
    }

    return {getInputData, map, reduce, summarize}

});