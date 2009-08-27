var cfg={filter4NameIgnoreCase:false,filter4NameAsRegExp:false};
var togglefilter4NameOptions=function(A){cfg[A]=!cfg[A];
$.cookie(A,cfg[A]);
$("input.option_"+A+"_cb").each(function(){this.checked=cfg[A]
});
updateFilter4NameRE()
};
$(document).ready(function(){for(optionName in cfg){cfg[optionName]=$.cookie(optionName);
cfg[optionName]=(cfg[optionName]==true||cfg[optionName]=="true");
$("input.option_"+optionName+"_cb").each(function(){this.checked=cfg[optionName]
})
}});
var filter4Packages=[];
var updateFilter4Packages=function(B){filter4Packages=[];
var A=$("#packagesFilter").get(0);
for(var C=0;
C<A.options.length;
C++){if(A.options[C].selected==true){filter4Packages.push(A.options[C].text)
}}updateClassesDisplay()
};
var checkFilter4Packages=function(B){if(filter4Packages.length<1){return true
}var A=B.attr("package");
return(jQuery.inArray(A,filter4Packages)!=-1)
};
var filter4Kind=[];
var maxKind=0;
var toggleFilter4Kind=function(A){var C=A.data;
var B=jQuery.inArray(C,filter4Kind);
if(B>-1){filter4Kind.splice(B,1)
}else{filter4Kind.push(C)
}$("#filter_"+C+"_cb").get(0).checked=(B<0);
updateClassesDisplay()
};
var checkFilter4Kind=function(B){if(filter4Kind.length==maxKind){return true
}var A=B.attr("class");
return(jQuery.inArray(A,filter4Kind)!=-1)
};
var filter4NameRE=null;
var filter4Name="";
var updateFilter4Name=function(A){filter4Name=this.value;
updateFilter4NameRE()
};
var updateFilter4NameRE=function(){if((filter4Name==null)||(filter4Name.length==0)){filter4NameRE=null
}else{var A=(cfg.filter4NameIgnoreCase)?"i":"";
var B=(cfg.filter4NameAsRegExp)?filter4Name:"^"+filter4Name;
filter4NameRE=new RegExp(B,A)
}updateClassesDisplay()
};
var checkFilter4Name=function(B){if(filter4NameRE==null){return true
}var A=B.children("a").text();
return filter4NameRE.test(A)
};
var lastUpdateClassDisplayCallId=null;
var updateClassesDisplay=function(){if(lastUpdateClassDisplayCallId!=null){clearTimeout(lastUpdateClassDisplayCallId)
}lastUpdateClassDisplayCallId=setTimeout("updateClassesDisplayNow()",300)
};
var updateClassesDisplayNow=function(){$("#classes li").each(function(){var A=$(this);
if(checkFilter4Packages(A)&&checkFilter4Kind(A)&&checkFilter4Name(A)){A.show()
}else{A.hide()
}})
};
$(document).ready(function(){$("#packagesFilter").each(function(){for(var A=0;
A<this.options.length;
A++){this.options[A].selected=false
}}).bind("change",updateFilter4Packages);
$("#kindFilters a").each(function(){var B=$(this);
var A=B.attr("id").substring("filter_".length);
B.bind("click",A,toggleFilter4Kind);
filter4Kind.push(A);
$("#filter_"+A+"_cb").get(0).checked=true;
maxKind++
});
$("#nameFilter").val("");
$("#nameFilter").bind("keyup",updateFilter4Name)
});
jQuery.fn.selectOptions=function(A){this.each(function(){if(this.nodeName.toLowerCase()!="select"){return 
}var B=this.options.length;
for(var C=0;
C<B;
C++){this.options[C].selected=(this.options[C].text==A)
}});
return this
};
var selectPackage=function(A){$("#packagesFilter").selectOptions(A);
updateFilter4Packages()
};
jQuery.cookie=function(B,I,L){if(typeof I!="undefined"){L=L||{};
if(I===null){I="";
L.expires=-1
}var E="";
if(L.expires&&(typeof L.expires=="number"||L.expires.toUTCString)){var F;
if(typeof L.expires=="number"){F=new Date();
F.setTime(F.getTime()+(L.expires*24*60*60*1000))
}else{F=L.expires
}E="; expires="+F.toUTCString()
}var K=L.path?"; path="+(L.path):"";
var G=L.domain?"; domain="+(L.domain):"";
var A=L.secure?"; secure":"";
document.cookie=[B,"=",encodeURIComponent(I),E,K,G,A].join("")
}else{var D=null;
if(document.cookie&&document.cookie!=""){var J=document.cookie.split(";");
for(var H=0;
H<J.length;
H++){var C=jQuery.trim(J[H]);
if(C.substring(0,B.length+1)==(B+"=")){D=decodeURIComponent(C.substring(B.length+1));
break
}}}return D
}};
