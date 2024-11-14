package com.snowflake.dlsync.dependency;

import com.snowflake.dlsync.models.Script;
import com.snowflake.dlsync.models.ScriptDependency;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DependencyGraph {
    private DependencyExtractor dependencyExtractor;
    private Map<Script, Set<Script>> dagGraph;
    private Map<Script, Integer> inDegree;
    private Stack<Script> zeroInDegreeScripts = new Stack<>();
    public DependencyGraph() {
        dependencyExtractor = new DependencyExtractor();
        dagGraph = new HashMap<>();
        inDegree = new HashMap<>();
    }

    public void addNodes(List<? extends Script> nodes) {
        log.info("Building dependency graph of {} scripts.", nodes.size());
        dependencyExtractor.addScripts(nodes);
        for(Script script: nodes) {
            Set<Script> scriptDependencies = dependencyExtractor.extractScriptDependencies(script);
            for(Script dependency: scriptDependencies) {
                dagGraph.computeIfAbsent(dependency, k -> new HashSet<>()).add(script);
            }

            inDegree.put(script, scriptDependencies.size());
            if(scriptDependencies.size() == 0) {
                zeroInDegreeScripts.push(script);
            }
        }
        log.debug("Using the following dependency graph: {}", dagGraph);
    }

    public List<Script> topologicalSort() {
        log.info("Sorting scripts based on dependency ...");
        List<Script> sortedScript = new ArrayList<>(inDegree.size());
        while(!zeroInDegreeScripts.isEmpty()) {
            Script currentScript = zeroInDegreeScripts.pop();
            sortedScript.add(currentScript);
            log.debug("{} script edges {} ", currentScript, dagGraph.get(currentScript));
            if(dagGraph.get(currentScript) == null) {
                continue;
            }
            for(Script edge: dagGraph.get(currentScript)) {
                int before = inDegree.get(edge);
                inDegree.put(edge, inDegree.get(edge) - 1);
                if (inDegree.get(edge) == 0) {
                    zeroInDegreeScripts.push(edge);
                }
            }
        }
        if(inDegree.size() != sortedScript.size()) {
            log.error("DAG graph Error, input script size({}) is different than sequenced script size({})", inDegree.size(), sortedScript.size());
            for(Script script: inDegree.keySet()) {
                if(!sortedScript.contains(script)) {
                    log.warn("Dependencies for {} are: {}", script, dependencyExtractor.extractScriptDependencies(script));
                }
            }
            throw new RuntimeException("Sorting Error, Cyclic dependency detected. sorted script size is missing some scripts.");
        }
        log.info("Sorted scripts: {}", sortedScript);
        return sortedScript;
    }

    public void printDependencyGraph() {
        for(Script node: dagGraph.keySet()) {
            System.out.println("Script: " + node.getFullObjectName() + " depends on -> " + dagGraph.get(node).stream().map(s -> s.getFullObjectName()).collect(Collectors.toList()));
        }
    }

    public List<ScriptDependency> getDependencyList() {
        Set<ScriptDependency> dependencyList = new HashSet<>();
        for(Script dependency: dagGraph.keySet()) {
            for(Script node: dagGraph.get(dependency)) {
                //Remove self dependency for migration scripts
                if(node.getFullObjectName().equals(dependency.getFullObjectName())) {
                    continue;
                }
                ScriptDependency scriptDependency = new ScriptDependency(node, dependency);
                dependencyList.add(scriptDependency);
            }
        }
        return new ArrayList<>(dependencyList);
    }

    public Map<Script, Set<Script>> getDagGraph() {
        return dagGraph;
    }
}
