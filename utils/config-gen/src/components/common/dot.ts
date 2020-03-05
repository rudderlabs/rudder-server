import styled from 'styled-components';
import { is } from 'typescript-styled-is';

export const Dot = styled.div`
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: ${props =>
    props.active ? props.theme.color.primary100 : props.theme.color.grey200};
  ${is('active')`
    background: ${({ theme }) => theme.color.primary100};
  `}
`;
