package sixdegrees;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class Node {

	private final int id;

	private int dist;
	private List<Integer> adjacent = new ArrayList<Integer>();
	private Colors color = Colors.WHITE; // WHITE = 0, GRAY = 1, BLACK = 2

	public Node(int id) {
		this.id = id;
	}

	public Node(String str) {
		super();
		// convert from string to Node
		String half[] = str.split("\\s+");

		// id
		this.id = Integer.parseInt(half[0]);

		String parts[] = half[1].split(":");
		
		for (String s : parts[0].split(",")) {
			if (s.length() > 0) {
				this.adjacent.add(Integer.parseInt(s));
			}
		}

		// distance
		this.dist = Integer.parseInt(parts[1]);

		// color
		this.color = Colors.valueOf(parts[2]);

	}

	public int getDist() {
		return dist;
	}

	public void setDist(int d) {
		this.dist = d;
	}

	public List<Integer> getAdjacent() {
		return adjacent;
	}

	public void setAdjacent(List<Integer> adjacent) {
		this.adjacent = adjacent;
	}

	public Colors getColor() {
		return color;
	}

	public void setColor(Colors color) {
		this.color = color;
	}

	public int getId() {
		return id;
	}

	public Text getLine() {
		StringBuffer s = new StringBuffer();
		int index = 0;
		while (index < this.adjacent.size()) {
			if (index < this.adjacent.size() - 1) {
				s.append(this.adjacent.get(index)).append(",");
			} else {
				s.append(this.adjacent.get(index));
			}
			index++;
		}
	
		s.append(":");

		if (this.dist < Integer.MAX_VALUE) {
			s.append(this.dist).append(":");
		} else {
			s.append("Integer.MAX_VALUE").append(":");
		}

		s.append(this.color); 

		return new Text(s.toString());
	}

	public static void main(String args[]) {
		Node n = new Node("1 2,5:0:1:5");
		System.out.println("Node ID " + n.getId());
		System.out.println("Edges " + n.getAdjacent());
		System.out.println("Distance " + n.getDist());
		System.out.println("Color " + n.getColor());
		System.out.println(n.getLine());
	}
}
