/**
 * Created by wangqi on 2017/5/25.
 */
/**
 * This file is part of Qunee for HTML5.
 * Copyright (c) 2016 by qunee.com
 **/
$(function () {
    var graph = new Q.Graph('canvas');
    var model = graph.graphModel;
    var index = 0;
    var currentNode;
    var kafkaInfo = new Object();
    
    $("#right_panel").hide();
    $("#mid_panel").width("80%");
    $("#input_configuration").hide();
    
    $("#vertexConfig").click(function(){
    	$("#graph_panel").show();
    	$("#input_configuration").hide();
    	$("#mid_title").children("li").html("DAG");
    });
    
    $("#inputData").click(function(){
    	$("#input_configuration").show();
    	$("#graph_panel").hide();	
    	$("#mid_title").children("li").html("Kafka");
    });
    
    var styles = {};
    styles[Q.Styles.SELECTION_COLOR] = 'red';
    graph.styles = styles;
    
    graph.onElementCreated = function (element, evt, dragInfo) {
        Q.Graph.prototype.onElementCreated.call(this, element, evt, dragInfo);
        if (element instanceof Q.Edge) {
        	element.name = "";
            return;
        }
    }
    
    graph.onclick = function(evt) {
    	currentNode = graph.getElementByMouseEvent(evt);
    	$("#config").click(function(){
    		$("#mid_panel").width("45%");
        	$("#right_panel").show();	
            $(".event-type").remove();
            $(".event-processor").remove();
        	$("#node_name_input").val(currentNode.name);
        	for(var i=0;i<currentNode.eventTypes.length;i++){
        		var eventid = "event_type_input_" + i;
        		$("#add_event_type").before(
        	            "<div class='form-group event-type'>"+
        	                "<label>事件名称</label>"+
        	                "<input type='text' class='form-control' id="+eventid+" placeholder='Event Type'>"+
        	                "<div id='event_props'>"+
        	        				"<div class='form-group' id='event_prop'>"+
        	        				"<label>事件属性</label>"+
        	        				"<div class='add-event-prop' id="+i+"><a href='#'><i class='fa fa-plus-circle'></i></a></div>"+
        	        			"</div>"+
        	            "</div>");
        		$("#"+eventid).val(currentNode.eventTypes[i].name);
        		for(var j=0;j<currentNode.eventTypes[i].props.length;j++){
        			var propid = i + "_prop_name_input_" + j;
        			var classid = i + "_prop_class_input_" + j;
        			$(".add-event-prop#"+i).before(
        	                "<div class='prop-inline'>"+
        	                    "<label>属性名称</label>"+
        	                    "<input type='text' class='form-control' id="+propid+" placeholder='Property Name'>"+
        	                    "<label>数据类型</label>"+
        	                    "<select class='form-control' id="+classid+">"+
        	                        "<option></option>"+
        	                        "<option>String</option>"+
        	                        "<option>int</option>"+
        	                        "<option>boolean</option>"+
        	                        "<option>float</option>"+
        	                        "<option>double</option>"+
        	                    "</select>"+
        	                "</div>");
        			$("#"+propid).val(currentNode.eventTypes[i].props[j].name);
        			$("#"+classid).val(currentNode.eventTypes[i].props[j].className);
        		}
        	}
        	for(var i=0;i<currentNode.processors.length;i++){
                var processorid = "event_process_input_" + i;
        		$("#add_event_processor").before(
        				"<div class='form-group event-processor'>"+
                        "<label>操作</label>"+
                        "<input type='text' class='form-control' id="+processorid+" placeholder='Event Processor'>"+
                    "</div>"		
        		);
        		$("#"+processorid).val(currentNode.processors[i]);
        	}
        	$("#out_type").val(currentNode.outEventType);
        	
        });	
    }
    
    
    
    $(".btn-group").children(".btn").click(function () {
        $(this).addClass("active");
        $(this).siblings().removeClass("active");
    });

    //custom interaction mode
    Q.Consts.INTERACTION_MODE_DELETE_NODE = 'interaction.mode.delete';
    Q.Defaults.registerInteractions(Q.Consts.INTERACTION_MODE_DELETE_NODE, [Q.PanInteraction, Q.WheelZoomInteraction, Q.SelectionInteraction, {
        onclick: function(evt, graph){
            graph.removeSelectionByInteraction(evt);
        }
    }]);

    $("#add").click(function(){
        var node = new Q.Node();
         index++;
         node.name = "vertex" + index;
         node.image = "node.svg";
         node.eventTypes = new Array();
         node.processors = new Array();
         node.outEventType = "";
         model.add(node);
    });

    $("#remove").click(function () {
        graph.interactionMode = Q.Consts.INTERACTION_MODE_DELETE_NODE;
    });

    $("#edge").click(function () {
        graph.interactionMode = Q.Consts.INTERACTION_MODE_CREATE_EDGE;
    });

    $("#default").click(function () {
    	graph.interactionMode = Q.Consts.INTERACTION_MODE_DEFAULT;
    });
    
	$("#add_event_type").click(function () {
		var event = new Object();
		event.props = new Array();
		var eventNum = currentNode.eventTypes.length;
        var eventid = "event_type_input_" + eventNum;
        currentNode.eventTypes.push(event);
        $(this).before(
            "<div class='form-group event-type'>"+
                "<label>事件名称</label>"+
                "<input type='text' class='form-control' id="+eventid+" placeholder='Event Type'>"+
                "<div id='event_props'>"+
        				"<div class='form-group' id='event_prop'>"+
        				"<label>事件属性</label>"+
        				"<div class='add-event-prop' id="+eventNum+"><a href='#'><i class='fa fa-plus-circle'></i></a></div>"+
        			"</div>"+
            "</div>");
        $(".add-event-prop").click(function () {
    		var eventId = $(this).attr("id");
    		var prop = new Object();
    		var propNum = currentNode.eventTypes[eventId].props.length;
    		var propid = eventId + "_prop_name_input_" + propNum;
    		var classid = eventId + "_prop_class_input_" + propNum;
    		currentNode.eventTypes[eventId].props.push(prop);
    		$(this).before(
    			"<div class='prop-inline'>"+
    				"<label>属性名称</label>"+
    				"<input type='text' class='form-control' id="+propid+" placeholder='Property Name'>"+
    				"<label>数据类型</label>"+
    				"<select class='form-control' id="+classid+">"+
    					"<option></option>"+
    					"<option>String</option>"+
    					"<option>int</option>"+
    					"<option>boolean</option>"+
                    	"<option>float</option>"+
                    	"<option>double</option>"+
                    	"</select>"+
    			"</div>");
    });
        
    });
    
    $("#add_event_processor").click(function () {
        var processorid = "event_process_input_" + currentNode.processors.length;
        currentNode.processors.push("");
        $(this).before(
            "<div class='form-group event-processor'>"+
                "<label>操作</label>"+
                "<input type='text' class='form-control' id="+processorid+" placeholder='Event Processor'>"+
            "</div>");
    });
    
    $("#save").click(function () {
        currentNode.name = $("#node_name_input").val();
        currentNode.outEventType = $("#out_type").val();
        for(var i=0;i<currentNode.processors.length;i++){
            currentNode.processors[i] = $("#event_process_input_"+i).val();
        }
        
        for(var i=0;i<currentNode.eventTypes.length;i++){ 
        	currentNode.eventTypes[i].name = $("#event_type_input_"+i).val();
    		for(var j=0;j<currentNode.eventTypes[i].props.length;j++){
    			currentNode.eventTypes[i].props[j].name = $("#"+i+"_prop_name_input_"+j).val();
    			currentNode.eventTypes[i].props[j].className = $("#"+i+"_prop_class_input_"+j).val();
    		}
        }
        
        $("#right_panel").hide();
        $("#mid_panel").width("80%");
        
    });
    
    $("#kafka_save").click(function(){
    	kafkaInfo.server = $("#kafka_server").val();
    	kafkaInfo.topics = $("#topic_list").val();
    });

    $("#submit").click(function () {
    	
    	var data = "{\"vertex\":[";
    	model.forEachByTopoBreadthFirstSearch(function(node){
    		var children = "";
    		node.forEachOutEdge(function(edge){
    			children = children + edge.to + ","
    		});
    		
    		var event_types = "[";
    		var epls = "[";
    		
    		for(var i=0;i<node.eventTypes.length;i++){
    			event_types = event_types + "{\"event_type\":\"" + node.eventTypes[i].name + "\",";
    			var event_props = "\"event_props\":[";
    			var event_classes = "\"event_classes\":["
    			for(var j=0;j<node.eventTypes[i].props.length;j++){
    				event_props = event_props + "\"" + node.eventTypes[i].props[j].name + "\",";
    				event_classes = event_classes + "\"" + node.eventTypes[i].props[j].className + "\",";
    			}
    			event_props = event_props + "],";
    			event_classes = event_classes + "]";
    			event_types = event_types + event_props + event_classes + "},"
    		}
    		event_types = event_types + "]";
    		
    		for(var i=0;i<node.processors.length;i++){
    			epls = epls + "\"" + node.processors[i] + "\",";
    		}
    		epls = epls + "]"
    		
            data = data + "{" +
            		"\"id\":" + node.id + "," +
            		"\"name\":\"" + node.name + "\"," +
            		"\"event_types\":" + event_types + "," +
            		"\"epl\":" + epls + "," +
            		"\"out_type\":\"" + node.outEventType + "\"," +
            		"\"children\":[" + children + "]" +
            		"},"
        });
    	data = data + "],\"kafka_server\":\"" + 
    	kafkaInfo.server + "\",\"kafka_topics\":\"" + kafkaInfo.topics + "\"}";
    	
    	$.ajax({
			type : "POST",
			url : "/dag",
			data : {data:data},
			dataType : "json",
			success : function(data) {
				console.log("SUCCESS: ", data);
			},
			error : function(e) {
				console.log("ERROR: ", e);
			},
			done : function(e) {
				console.log("DONE");
			}
		});

    });

});