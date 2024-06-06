import java.util.*;
import java.util.concurrent.*;

public class Raft {
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);  // shared executor service
    enum NodeState {
        LEADER, FOLLOWER, CANDIDATE
    }

    class Entry {
        String command;
        int index, term;
        public Entry(String command, int index, int term) {
            this.command = command;
            this.index = index;
            this.term = term;
        }
    }

    class PersistentState {
        Map<Integer, NodeStateData> nodeData = new HashMap<>();
        void saveState(Node node) {
            nodeData.put(node.id, new NodeStateData(node.state, node.termNumber, node.log));
        }
        NodeStateData getState(int nodeId) {
            return nodeData.getOrDefault(nodeId, null);
        }
    }

    class NodeStateData {
        NodeState state;
        int termNumber;
        List<Entry> log;
        public NodeStateData(NodeState state, int termNumber, List<Entry> log) {
            this.state = state;
            this.termNumber = termNumber;
            this.log = new ArrayList<>(log);
        }
    }

    class Node {
        int id, termNumber, commitIndex = 0;
        NodeState state;
        transient ScheduledFuture<?> timeoutTask;
        List<Entry> log;
        public Node(int id) {
            this.id = id;
            this.state = NodeState.FOLLOWER;
            this.termNumber = 0;
            this.log = new ArrayList<>();
            resetTimeout();
        }
        public boolean requestVotes(int term, int candidateId) {
            if (term <= this.termNumber) return false;
            this.termNumber = term;
            this.state = NodeState.FOLLOWER;
            return true;
        }
        public boolean appendEntries(List<Entry> entries, int term, int prevLogIndex, int prevLogTerm) {
            if (term < this.termNumber || log.size() <= prevLogIndex || log.get(prevLogIndex).term != prevLogTerm) return false;
            for (int i = 0; i <= prevLogIndex; i++) {
                if (i >= log.size() || i >= entries.size()) break;
                if (log.get(i).term != entries.get(i).term) return false;
            }
            resetTimeout();
            for (int i = prevLogIndex + 1; i < entries.size(); i++) {
                if (log.size() > i && log.get(i).term != entries.get(i).term) log.subList(i, log.size()).clear();
                if (log.size() == i) log.add(entries.get(i));
            }
            return true;
        }
        public boolean heartbeat(int term) {
            if (term < this.termNumber) return false;
            resetTimeout();
            this.termNumber = term;
            return true;
        }
        public void applyCommittedEntries() {
            while (commitIndex < log.size()) {
                Entry entry = log.get(commitIndex);
                System.out.println("Node " + id + " executed: " + entry.command);
                commitIndex++;
            }
        }
        private void resetTimeout() {
            if (timeoutTask != null) timeoutTask.cancel(true);
            timeoutTask = executorService.schedule(() -> {
                if (state != NodeState.LEADER) electLeader();
            }, new Random().nextInt(150) + 150, TimeUnit.MILLISECONDS);
        }

        private void compactLog() {
            if (log.size() > 100) log.clear();
        }
    }

    List<Node> nodes;
    PersistentState persistentState = new PersistentState();
    public Raft(int nodeCount) {
        this.nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            NodeStateData stateData = persistentState.getState(i);
            if (stateData != null) {
                Node node = new Node(i);
                node.state = stateData.state;
                node.termNumber = stateData.termNumber;
                node.log = stateData.log;
                nodes.add(node);
            } else {
                nodes.add(new Node(i));
            }
        }
    }
    private boolean preVote(Node candidate) {
        int indicationCount = 0;
        for (Node node : nodes) {
            if (node.log.size() <= candidate.log.size()) indicationCount++;
        }
        return indicationCount > nodes.size() / 2;
    }
    public void clientRequest(String command) {
        Node leader = findLeader();
        if (leader != null) {
            addCommand(command);
            System.out.println("Command " + command + " added and will be committed soon.");
        } else {
            System.out.println("No leader found. Please retry later.");
        }
    }
    public void electLeader() {
        Node candidate = nodes.get(new Random().nextInt(nodes.size()));
        candidate.state = NodeState.CANDIDATE;
        int voteCount = 1;
        for (Node node : nodes) {
            if (node != candidate && node.requestVotes(candidate.termNumber + 1, candidate.id)) voteCount++;
        }
        if (voteCount > nodes.size() / 2) {
            candidate.state = NodeState.LEADER;
            candidate.termNumber++;
            persistentState.saveState(candidate);
            sendHeartbeats(candidate);
        } else {
            candidate.state = NodeState.FOLLOWER;
            persistentState.saveState(candidate);
        }
    }
    private void sendHeartbeats(Node leader) {
        executorService.scheduleAtFixedRate(() -> {
            for (Node node : nodes) {
                if (node != leader) node.heartbeat(leader.termNumber);
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void addCommand(String command) {
        Node leader = findLeader();
        if (leader != null) {
            Entry entry = new Entry(command, leader.log.size() + 1, leader.termNumber);
            leader.log.add(entry);
            int replicatedCount = 1;
            int prevLogIndex = leader.log.size() - 1;
            int prevLogTerm = prevLogIndex >= 0 ? leader.log.get(prevLogIndex).term : 0;

            for (Node node : nodes) {
                if (node != leader) {
                    List<Entry> pendingEntries = (prevLogIndex >= 0) ? leader.log.subList(prevLogIndex, leader.log.size()) : new ArrayList<>();
                    while (!node.appendEntries(pendingEntries, leader.termNumber, prevLogIndex, prevLogTerm) && prevLogIndex > 0) {
                        prevLogIndex--;
                        prevLogTerm = leader.log.get(prevLogIndex).term;
                        pendingEntries = leader.log.subList(prevLogIndex, leader.log.size());
                    }
                    replicatedCount++;
                }
            }

            if (replicatedCount > nodes.size() / 2) {
                leader.commitIndex = leader.log.size();
                leader.applyCommittedEntries();
            }
        } else {
            System.out.println("No leader to add the command to!");
        }
    }
    public Node findLeader() {
        for (Node node : nodes) {
            if (node.state == NodeState.LEADER) return node;
        }
        return null;
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Executor service did not terminate");
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    public static void main(String[] args) {
        Raft raftCluster = new Raft(5);
        raftCluster.electLeader();
        raftCluster.clientRequest("WRITE DATA");
        raftCluster.shutdown();
    }
}
