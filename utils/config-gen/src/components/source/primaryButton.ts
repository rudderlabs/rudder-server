import { Button } from '@components/common/button';
import styled from 'styled-components';

export const PrimaryButton = styled(Button)`
  margin: 10px 0 10px 0px;
  background: transparent !important;
  border: 2px solid ${props => props.theme.color.primary};
  color: ${props => props.theme.color.primary} !important;
  font-weight: bold;
`;
