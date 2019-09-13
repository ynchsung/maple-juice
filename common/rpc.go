package common

type Args struct {
	A int
	B int
}

type Reply struct {
	A int
	B int
}

type YCSW struct {
	A int
	B int
}

func (t *YCSW) Test(args *Args, reply *Reply) error {
	reply.A = args.B * args.B
	reply.B = args.A * args.A
	return nil
}
